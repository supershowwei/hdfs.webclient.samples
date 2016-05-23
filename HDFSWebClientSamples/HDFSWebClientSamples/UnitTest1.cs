using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

            var directories = this.ListDirectories();

            var files = this.ListFiles();

            this.CreateDirectory();

            this.UploadFile();

            this.DownloadFile();

            this.DeleteFile();

            this.DeleteDirectory();
        }

        private WebHDFSClient CreateWebHDFSClient()
        {
            return new WebHDFSClient(new Uri("HDFS Name Node Uri"), "Hadoop Account");
        }

        private IEnumerable<string> ListDirectories()
        {
            // 要操作 /user 底下的目錄及檔案，必須先給予我們的 Hadoop 帳號權限。
            var dirStatus = this.webHDFSClient.GetDirectoryStatus("/user 底下的相對路徑").Result;

            return dirStatus.Directories.Select(d => $"/{d.PathSuffix}");
        }

        private IEnumerable<string> ListFiles()
        {
            var dirStatus = this.webHDFSClient.GetDirectoryStatus("/user 底下的相對路徑").Result;

            return dirStatus.Files.Select(d => $"/{d.PathSuffix}");
        }

        private bool CreateDirectory()
        {
            string targetDirectory = "目錄在 /user 底下欲擺放的相對路徑";

            return this.webHDFSClient.CreateDirectory(targetDirectory).Result;
        }

        private string UploadFile()
        {
            string targetFilePath = "檔案在 /user 底下欲擺放的相對路徑";

            return this.webHDFSClient.CreateFile("要上傳的檔案路徑", targetFilePath).Result;
        }

        private void DownloadFile()
        {
            string fileOpened = "欲下載的檔案在 /user 底下擺放的相對路徑";

            var response = this.webHDFSClient.OpenFile(fileOpened).Result;
            var stream = response.Content.ReadAsStreamAsync().Result;

            using (FileStream fs = File.Create("欲儲存的絕對路徑"))
            {
                stream.CopyTo(fs);
            }
        }

        private bool DeleteFile()
        {
            string targetFilePath = "欲刪除的檔案在 /user 底下擺放的相對路徑";

            // Microsoft.Hadoop.WebHDFS 沒有 DeleteFile 的方法，無論刪除檔案或是資料夾都是透過 DeleteDirectory 來操作。
            return this.webHDFSClient.DeleteDirectory(targetFilePath).Result;
        }

        private bool DeleteDirectory()
        {
            string targetDirectory = "欲刪除的目錄在 /user 底下擺放的相對路徑";

            // 欲刪除非空資料夾，須將 recursive 設為 true，預設為 false。
            return this.webHDFSClient.DeleteDirectory(targetDirectory, true).Result;
        }
    }
}
