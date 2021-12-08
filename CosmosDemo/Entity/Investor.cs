using System;

namespace CosmosDemo
{
    public class Investor
    {
        public string id { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }
        public DateTime CreatedDate { get; set; }
        public string CountryId { get; set; }
        public string PartitionKey { get; set; }
    }
}
