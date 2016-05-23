using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HDFSWebClientSamples
{
    class HARIndexEntry
    {
        public string PathSuffix { get; set; }

        public string Type { get; set; }

        public string PartFileName { get; set; }

        public int Offset { get; set; }

        public int Length { get; set; }
    }
}
