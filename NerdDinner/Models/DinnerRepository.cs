using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Objects.DataClasses;
using System.Data;
using CorrugatedIron;
using CorrugatedIron.Comms;
using CorrugatedIron.Config;
using CorrugatedIron.Models;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.MapReduce.Inputs;
using CorrugatedIron.Util;
using CorrugatedIron.Extensions;
using Newtonsoft.Json;

namespace NerdDinner.Models
{

    public class DinnerRepository : IDinnerRepository
    {
    	readonly IRiakEndPoint _cluster;
    	readonly IRiakClient _client;
		readonly IRiakClusterConfiguration _clusterConfig = RiakClusterConfiguration.LoadFromConfig("riakLoadTestConfiguration");
    	private const string DinnerBucket = "RiakNerdDinner_dinner";

    	public DinnerRepository()
    	{
			_cluster = new RiakCluster(_clusterConfig, new RiakConnectionFactory());
			_client = _cluster.CreateClient();
    	}

        //
        // Query Methods

        public IQueryable<Dinner> FindDinnersByText(string q)
        {
			var mr = new RiakMapReduceQuery { ContentType = RiakConstants.ContentTypes.ApplicationJson };

        	mr.Inputs(DinnerBucket)
        		.MapJs(m => m.Source(@"
function (value, keyData, arg) {
	var o = Riak.mapValuesJson(value)[0];

	if (o.Title.Contains(arg.keyword) 
		|| o.Description.Contains(arg.keyword) 
		|| o.HostedBy.Contains(arg.keyword))
	{
		return [o];
	}
}
").Argument(q).Keep(true));

			// TODO create a version of this using RiakSearch

        	var mrResult = _client.MapReduce(mr);
			if (mrResult.IsSuccess)
			{
				var results = mrResult.Value.PhaseResults.Last().GetObjects<Dinner>();
				return results.AsQueryable();
			}

			throw new ApplicationException(mrResult.ErrorMessage);
        }

        public IQueryable<Dinner> FindAllDinners()
        {
        	var keys = _client.ListKeys(DinnerBucket);
        	var oids = new List<RiakObjectId>();
        	foreach (var key in keys.Value)
        	{
        		oids.Add(new RiakObjectId(DinnerBucket, key));
        	}

			var rVal = new List<Dinner>();

        	var objects = _client.Get(oids);
        	foreach (var riakResult in objects)
        	{
        		rVal.Add(riakResult.Value.GetObject<Dinner>());
        	}

        	return rVal.AsQueryable();
        }

        public IQueryable<Dinner> FindUpcomingDinners()
        {
			var mr = new RiakMapReduceQuery { ContentType = RiakConstants.ContentTypes.ApplicationJson };

			// TODO: Modify DinnerId to be a string.

        	var input = new RiakIntIndexRangeInput(DinnerBucket, "EventDate_int", int.Parse(DateTime.Now.ToString("yyyyMMdd")), int.MaxValue);

        	mr.Inputs(input)
        		.MapJs(m => m.Name("Riak.mapValuesJson").Keep(true));

			return MapReduceDinners(mr);

        	//return from dinner in FindAllDinners()
			//       where dinner.EventDate >= DateTime.Now
			//       orderby dinner.EventDate
			//       select dinner;
        }


    	public IQueryable<Dinner> FindByLocation(float latitude, float longitude)
        {
			var mr = new RiakMapReduceQuery { ContentType = RiakConstants.ContentTypes.ApplicationJson };

        	var input = new RiakIntIndexRangeInput(DinnerBucket, "EventDate_int", int.Parse(DateTime.Now.ToString("yyyyMMdd")), int.MaxValue);

			mr.Inputs(input)
				.MapJs(m => m.Name("Riak.mapValuesJson").Keep(false))
				.MapJs(
					m =>
					m.Source(
						@"
function (value, keyData, arg) {
	// From http://stackoverflow.com/questions/27928/how-do-i-calculate-distance-between-two-latitude-longitude-points
	var lat, long = arg.split(""|"");

	var R = 6371; // Radius of the earth in km
	var dLat = (lat2-lat).toRad();  // Javascript functions in radians
	var dLon = (lon2-lon).toRad(); 
	var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
			Math.cos(lat1.toRad()) * Math.cos(lat2.toRad()) * 
			Math.sin(dLon/2) * Math.sin(dLon/2); 
	var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
	var d = R * c; // Distance in km
	var m = d * 1.609344; // Distance in miles

	if (m < 100) {
		return [value];
	}
	else
	{
		return [];
	}
}
")
						.Argument(string.Format("{0}|{1}", latitude, longitude))
						.Keep(true));

			return MapReduceDinners(mr);

			//var dinners = from dinner in FindUpcomingDinners()
			//              join i in NearestDinners(latitude, longitude)
			//              on dinner.DinnerID equals i.DinnerID
			//              select dinner;

			//return dinners;
        }

        public Dinner GetDinner(string id)
        {
        	var result = _client.Get(DinnerBucket, id);
        	var value = result.Value;
        	return value.GetObject<Dinner>();
        	//return _client.Get(DinnerBucket, id.ToString()).Value.GetObject<Dinner>();
        }

        //
        // Insert/Delete Methods

        public void Add(Dinner dinner)
        {
        	SaveDinner(dinner);
        }

        public void Delete(Dinner dinner)
        {
			//foreach (RSVP rsvp in dinner.RSVPs.ToList())
			//    db.RSVPs.DeleteObject(rsvp);
			//db.Dinners.DeleteObject(dinner);
        	var roi = new RiakObjectId(DinnerBucket, dinner.DinnerID);

			_client.Delete(roi);
        }

        //
        // Persistence 

        public void Save()
        {
			//_db.SaveChanges();
        }

		public void SaveDinner(Dinner dinner)
		{
			var ro = new RiakObject(DinnerBucket, dinner.DinnerID, dinner);
			ro.AddIntIndex("EventDate_int", dinner.EventDateToInt());

			_client.Put(ro);
		}

		//
        // Helper Methods

        [EdmFunction("NerdDinnerModel.Store", "DistanceBetween")]
        public static double DistanceBetween(double lat1, double long1, double lat2, double long2)
        {
            throw new NotImplementedException("Only call through LINQ expression");
        }

        public IQueryable<Dinner> NearestDinners(double latitude, double longitude)
        {
        	var mr = new RiakMapReduceQuery {ContentType = RiakConstants.ContentTypes.ApplicationJson};

        	mr.Inputs(DinnerBucket)
        		.MapJs(
        			m =>
        			m.Source(
        				@"
function (value, keyData, arg) {
	// From http://stackoverflow.com/questions/27928/how-do-i-calculate-distance-between-two-latitude-longitude-points
	var lat, long = arg.split(""|"");

	var R = 6371; // Radius of the earth in km
	var dLat = (lat2-lat).toRad();  // Javascript functions in radians
	var dLon = (lon2-lon).toRad(); 
	var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
			Math.cos(lat1.toRad()) * Math.cos(lat2.toRad()) * 
			Math.sin(dLon/2) * Math.sin(dLon/2); 
	var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
	var d = R * c; // Distance in km
	var m = d * 1.609344; // Distance in miles

	if (m < 100) {
		return [value];
	}
	else
	{
		return [];
	}
}
")
        				.Argument(string.Format("{0}|{1}", latitude, longitude))
						.Keep(true));

			return MapReduceDinners(mr);

			//return from d in _db.Dinners
			//       where DistanceBetween(latitude, longitude, d.Latitude, d.Longitude) < 100
			//       select d;
        }

		private IQueryable<Dinner> MapReduceDinners(RiakMapReduceQuery mr)
		{
			var mrResult = _client.MapReduce(mr);
			if (mrResult.IsSuccess)
			{
				var results = mrResult.Value.PhaseResults.Last().GetObjects<Dinner>();
				return results.AsQueryable();
			}

			throw new ApplicationException(mrResult.ErrorMessage);
		}
    }	
}
