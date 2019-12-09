using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
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
                List<FileChunk> chunks = new List<FileChunk>();
                var consumer = new EventingBasicConsumer(channel);

                string path = @"C:\Users\Zhanibek\Desktop\data34.bin";

                using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;

                        var message = Encoding.UTF8.GetString(body);
                        Type type = typeof(FileChunk);
                        FileChunk fileChunk = JsonConvert.DeserializeObject(message, type) as FileChunk;
                        chunks.Add(fileChunk);
                        //Console.WriteLine(" [x] Received {0} {1} {2} \n",fileChunk.FileId, fileChunk.Content, fileChunk.ChunkNo);

                        Console.WriteLine(fs.Length);
                        fs.Write(fileChunk.Content, 0 + (fileChunk.ChunkNo - 1) * (fileChunk.Content.Length / 1024 / 1024), fileChunk.Content.Length);
                        Console.WriteLine("________\n" + fs.Length);




                    }; chunks.OrderBy(o => o.ChunkNo);
                    channel.BasicConsume(queue: "files_to_process_queue",
                                             autoAck: true,
                                             consumer: consumer);
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}
