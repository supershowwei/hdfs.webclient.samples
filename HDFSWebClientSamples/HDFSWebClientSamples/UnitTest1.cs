using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;
using Microsoft.Hadoop.WebHDFS;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace HDFSWebClientSamples
{
    [TestClass]
    public class UnitTest1
    {
        WebHDFSClient webHDFSClient;

        [TestMethod]
        public void TestMethod1()
        {
            this.webHDFSClient = this.CreateWebHDFSClient();

            var files = this.ListFilesInHAR();

            this.DownloadFileInHAR();
        }

        private WebHDFSClient CreateWebHDFSClient()
        {
            return new WebHDFSClient(new Uri("HDFS Name Node Uri"), "Hadoop Account");
        }

        private IEnumerable<string> ListFilesInHAR()
        {
            string indexFilePath = "擺放在 HAR 檔底下 _index 檔案的相對路徑";

            // 讀取 _index 檔案，並將檔案內容做 UrlDecode。
            var response = this.webHDFSClient.OpenFile(indexFilePath).Result;
            var indexFileStream = response.Content.ReadAsStreamAsync().Result;
            string indexData = Encoding.UTF8.GetString(((MemoryStream)indexFileStream).ToArray());
            string decodedIndexData = HttpUtility.UrlDecode(indexData);

            // 轉換 _index 檔案內容為 HARIndexEntry
            var indexEntries = new List<HARIndexEntry>();
            var decodedIndexDataLines = decodedIndexData.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

            // 從第 2 列開始，跳過第 1 列。
            for (int lineIndex = 1; lineIndex < decodedIndexDataLines.Length; lineIndex++)
            {
                var indexEntryDataArray = decodedIndexDataLines[lineIndex].Split(' ');

                indexEntries.Add(
                    new HARIndexEntry()
                    {
                        PathSuffix = indexEntryDataArray[0],
                        Type = indexEntryDataArray[1],
                        PartFileName = indexEntryDataArray[2],
                        Offset = int.Parse(indexEntryDataArray[3]),
                        Length = int.Parse(indexEntryDataArray[4])
                    });
            }

            return indexEntries.Select(t => t.PathSuffix.Substring(1));
        }

        private void DownloadFileInHAR()
        {
            // 這邊要注意的是我們要 Open 的 File 已經不是我們熟悉的檔案名稱，而是我們欲下載的檔案所屬的 part 檔名。
            string fileOpened = "擺放在 HAR 檔底下，我們欲下載的檔案所屬的 part 檔名。";
            int fileOffset = 280060;
            int fileLength = 144187; 

            // Open File 的時候我們要多丟 2 個參數 - offset、length，而這 2 個參數在 _index 檔都寫得清清楚楚。
            var response = this.webHDFSClient.OpenFile(fileOpened, fileOffset, fileLength).Result;
            var stream = response.Content.ReadAsStreamAsync().Result;

            using (FileStream fs = File.Create("欲儲存的絕對路徑"))
            {
                stream.CopyTo(fs);
            }
        }
    }
}
