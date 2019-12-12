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

                    int readCount = fs.Read(bytes, 0, bytes.Length); ;
                    partitionsCount++;


                    Console.WriteLine(fs.Position.ToString());
                    while ((readCount == bytes.Length||readCount<bytes.Length)&&readCount!=0)
                    {
                        FileChunk filechunk = new FileChunk()
                        {
                            FileId = id,
                            Content = bytes,
                            ChunkNo = partitionsCount,
                            Position = fs.Position
                        };
                        var containerAsJson = JsonConvert.SerializeObject(filechunk);
                        var body = Encoding.UTF8.GetBytes(containerAsJson);
                        channel.BasicPublish(exchange: "",
                                             routingKey: "files_to_process_queue",
                                             basicProperties: null,
                                             body: body);

                        readCount = fs.Read(bytes, 0, bytes.Length);
                        Console.WriteLine(fs.Position.ToString());

                        partitionsCount++;
                    }
                }
                Console.ReadLine();
            }
        }
    }
}

