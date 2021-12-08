using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosDemo
{
    public class Country
    {
        public string id { get; set; }
        public string Type { get; set; }
        public string CountryCode { get; set; }
        public string CountryName { get; set; }
        public string PartitionKey { get; set; }
    }
}
