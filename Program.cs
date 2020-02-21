using System;
using Azure.Storage;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Azure;
using NUnit.Framework;
using System.Text;

namespace AzureMessageQueue
{
    class Program
    {
        static async Task  Main(string[] args)
        {
            string connectionString = "DefaultEndpointsProtocol=https;AccountName=mytestqueuesr;AccountKey=ExKHeNyVcb5TpmLQOjZCqyQqzLPRmM2iZYg4pgH3kZcIkSS3ZIKH5IBcvpQFXI9QXXseYuC0QSbW5csL9GUXjg==;BlobEndpoint=https://mytestqueuesr.blob.core.windows.net/;QueueEndpoint=https://mytestqueuesr.queue.core.windows.net/;TableEndpoint=https://mytestqueuesr.table.core.windows.net/;FileEndpoint=https://mytestqueuesr.file.core.windows.net/;";
            //string connectionString = "UseDevelopmentStorage=true";
            //await CreateQueueAsync(connectionString, "test");
            //string message = "This is my first message!";
            string message = "Скоро на рыбалку 2!!!";
            byte[] arr = Encoding.UTF8.GetBytes(message);
            string message64 = Convert.ToBase64String(arr);
            await SendMessageAsync(connectionString, "test", message64);
            //await SendMessageAsync(connectionString, "test", "The spring is coming!!!");
            //await PeekMessagesAsync(connectionString, "test");
        }

        static async Task SendMessageAsync(string connString, string queueName, string message)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connString);
            QueueClient queue = serviceClient.GetQueueClient(queueName);
            SendReceipt  receipt = await queue.SendMessageAsync(message);
            Console.WriteLine($"Сообщение успешно отправлено в очередь {queueName}"); 
        }

        static async Task SendMessageAsync(string connString, string queueName, string message, int TTL)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connString);
            QueueClient queue = serviceClient.GetQueueClient(queueName);
            await queue.SendMessageAsync(message, timeToLive: TimeSpan.FromSeconds(TTL));
            Console.WriteLine($"Сообщение успешно отправлено в очередь {queueName}");
        }

        static async Task PeekMessagesAsync(string connString, string queueName)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connString);
            QueueClient queue = serviceClient.GetQueueClient(queueName);
            foreach (PeekedMessage message in (await queue.PeekMessagesAsync(maxMessages: 10)).Value)
            {
                Console.WriteLine($"Id: {message.MessageId}");
                Console.WriteLine($"Text: {message.MessageText}");
                Console.WriteLine($"{message.DequeueCount}");
                Console.WriteLine("---------------");
            }
        }

        static async Task ReceiveMessageAsync(string connString, string queueName)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connString);
            QueueClient queue = serviceClient.GetQueueClient(queueName);
            foreach (QueueMessage message in (await queue.ReceiveMessagesAsync(maxMessages:10)).Value)
            {
                Console.WriteLine($"Id: {message.MessageId}");
                Console.WriteLine($"Text: {message.MessageText}");
                Console.WriteLine($"{message.DequeueCount}");
                Console.WriteLine("---------------");
            }
        }
        static async Task DeleteMessageAsync(string connString, string queueName)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connString);
            QueueClient queue = serviceClient.GetQueueClient(queueName);
            foreach (QueueMessage message in (await queue.ReceiveMessagesAsync(maxMessages: 10)).Value)
            {
                Console.WriteLine($"Id: {message.MessageId}");
                Console.WriteLine($"Text: {message.MessageText}");
                Console.WriteLine($"{message.DequeueCount}");
                Console.WriteLine("---------------");
                //моделируем обработку сообщения
                await Task.Delay(2000);
                await queue.DeleteMessageAsync(message.MessageId, message.PopReceipt);
            }
        }

       static async Task ReceiveAndUpdateAsync(string connectionString, string queueName)
       {
            QueueClient queue = new QueueClient(connectionString, queueName);
            await queue.SendMessageAsync("first");
            await queue.SendMessageAsync("second");
            await queue.SendMessageAsync("third");
            // получаем сообщения из очереди с коротким таймаутом видимости в 1 с
            List<QueueMessage> messages = new List<QueueMessage>();
            Response<QueueMessage[]> received = await queue.ReceiveMessagesAsync(10, visibilityTimeout: TimeSpan.FromSeconds(1));
            foreach (QueueMessage message in received.Value)
            {
                // Сообщаем сервису, что нам нужно немного больше времени на обработку сообщения,
                // указывая для него окно видимости в 5 с
                UpdateReceipt receipt = await queue.UpdateMessageAsync(
                    message.MessageId,
                    message.PopReceipt,
                    message.MessageText,
                    TimeSpan.FromSeconds(5));
                // Keep track of the updated messages
                messages.Add(message.Update(receipt));
            }
            // Ждем, пока исходное окно видимости в 1 с истечет и проверяем,
            // что сообщения еще не видимы
            //для этого повторно инициируем получение сообщений
            await Task.Delay(TimeSpan.FromSeconds(1.5));
            Assert.AreEqual(0, (await queue.ReceiveMessagesAsync(10)).Value.Length);
            // Finish processing the messages
            foreach (QueueMessage message in messages)
            {
                // "Обрабатываем" сообщения
                Console.WriteLine($"Message: {message.MessageText}");

                // Заканчиваем обработку сообщений, удаляя их и очереди
                await queue.DeleteMessageAsync(message.MessageId, message.PopReceipt);
            }
        }

        static async Task DeleteQueueAsync(string connectionString, string queueName)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connectionString);
            QueueClient queue = serviceClient.GetQueueClient(queueName);
            await queue.DeleteAsync();
        }


        static async Task CreateQueueAsync(string connectionString, string queueName)
        {
            QueueServiceClient serviceClient = new QueueServiceClient(connectionString);
            QueueClient queue=  await serviceClient.CreateQueueAsync(queueName);
        }

    }


}






