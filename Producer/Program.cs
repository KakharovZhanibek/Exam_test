using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Producer
{
    class FileChunk
    {
        public string FileId;
        public byte[] Content;
        public int ChunkNo;
        public long Position;
    }
    class Program
    {
 
        static void Main(string[] args)
        {

            string path = @"C:\Users\Zhanibek\Desktop\rise.mp4";
            string id = Guid.NewGuid().ToString();
            int partitionsCount = 0;
            var bytes = new byte[(1024 * 1024) * 4];



            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "files_to_process_queue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
                using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
                {
                    Console.WriteLine(fs.Length);
                    while (fs.Position != fs.Length)
                    {
                        FileChunk filechunk = new FileChunk();
                        if (fs.Length - fs.Position < bytes.Length)
                        {

                            Console.WriteLine("End: " + (fs.Length - fs.Position).ToString());
                            byte[] temp = new byte[fs.Length - fs.Position];
                            fs.Read(temp, 0, Convert.ToInt32(fs.Length - fs.Position));
                            partitionsCount++;

                            filechunk.FileId = id;
                            filechunk.Content = temp;
                            filechunk.ChunkNo = partitionsCount;
                            filechunk.Position = fs.Position;

                        }
                        else
                        {
                            fs.Read(bytes, 0, bytes.Length);
                            partitionsCount++;

                            filechunk.FileId = id;
                            filechunk.Content = bytes;
                            filechunk.ChunkNo = partitionsCount;
                            filechunk.Position = fs.Position;
                        }


                        Console.WriteLine(fs.Position.ToString());

                        var containerAsJson = JsonConvert.SerializeObject(filechunk);
                        var body = Encoding.UTF8.GetBytes(containerAsJson);
                        channel.BasicPublish(exchange: "",
                                             routingKey: "files_to_process_queue",
                                             basicProperties: null,
                                             body: body);

                    }
                    Console.ReadLine();
                }
            }
        }
        static void func(FileStream fs,byte[]bytes,int partitionCount,string id)
        {
            Console.WriteLine(fs.Length);
            while (fs.Position != fs.Length)
            {
                FileChunk filechunk = new FileChunk();
                if (fs.Length - fs.Position < bytes.Length)
                {

                    Console.WriteLine("End: " + (fs.Length - fs.Position).ToString());
                    byte[] temp = new byte[fs.Length - fs.Position];
                    fs.Read(temp, 0, Convert.ToInt32(fs.Length - fs.Position));
                    partitionsCount++;

                    filechunk.FileId = id;
                    filechunk.Content = temp;
                    filechunk.ChunkNo = partitionsCount;
                    filechunk.Position = fs.Position;

                }
                else
                {
                    fs.Read(bytes, 0, bytes.Length);
                    partitionsCount++;

                    filechunk.FileId = id;
                    filechunk.Content = bytes;
                    filechunk.ChunkNo = partitionsCount;
                    filechunk.Position = fs.Position;
                }


                Console.WriteLine(fs.Position.ToString());

                var containerAsJson = JsonConvert.SerializeObject(filechunk);
                var body = Encoding.UTF8.GetBytes(containerAsJson);
                channel.BasicPublish(exchange: "",
                                     routingKey: "files_to_process_queue",
                                     basicProperties: null,
                                     body: body);

            }
        }
    }
}

