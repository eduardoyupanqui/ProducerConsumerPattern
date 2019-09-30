using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace BlockingCollection
{
    class Program
    {
        /// <summary>
        /// https://www.dotnetcurry.com/patterns-practices/1407/producer-consumer-pattern-dotnet-csharp
        /// 
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            var processDocuments = new ProcessDocuments();

            //MainAsync(args).GetAwaiter().GetResult();
            //processDocuments.ProcessDocumentsUsingNoobsterMind();
            //processDocuments.ProcessDocumentsInParallel();
            //processDocuments.ProcessDocumentsInParallelSemaphore();
            //ProcessDocumentsInParallelSemaphore();
            //processDocuments.ProcessDocumentsUsingProducerConsumerPattern();
            processDocuments.ProcessDocumentsUsingPipelinePattern();
            Console.ReadLine();
        }
        static async Task MainAsync(string[] args)
        {
            Console.WriteLine("Hello World!");
        }

        

    }

    
}
