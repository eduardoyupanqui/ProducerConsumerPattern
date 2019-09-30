using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Diagnostics;

namespace BlockingCollection
{
    class Program
    {
        /// <summary>
        /// https://www.dotnetcurry.com/patterns-practices/1407/producer-consumer-pattern-dotnet-csharp
        /// https://www.dotnetcurry.com/patterns-practices/1412/dataflow-pattern-csharp-dotnet
        /// https://www.dotnetcurry.com/dotnetcore/1509/async-dotnetcore-pattern
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            var processDocuments = new ProcessDocuments();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            //processDocuments.ProcessDocumentsUsingNoobsterMind();//35s
            //processDocuments.ProcessDocumentsInParallel();//4400ms
            //processDocuments.ProcessDocumentsInParallelSemaphore();//4500ms
            //processDocuments.ProcessDocumentsUsingProducerConsumerPattern();//10s
            //processDocuments.ProcessDocumentsUsingPipelinePattern();//11s
            //processDocuments.ProcessDocumentsUsingDataflowPattern();//9000ms
            //processDocuments.ProcessDocumentsUsingTplDataflow();//20s
            //processDocuments.ProcessDocumentsUsingParallelForEach();//4400ms
            //processDocuments.ProcessDocumentsUsingProceduralDataflow();//20s
            //processDocuments.ProcessDocumentsUsingProducerConsumerPatternChannel();
            //MainAsync(processDocuments).GetAwaiter().GetResult();
            // Stop.
            stopwatch.Stop();

            // Write hours, minutes and seconds.
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
            Console.WriteLine("Time elapsed: {0} ms", stopwatch.ElapsedMilliseconds);
            Console.ReadLine();
        }
        static Task MainAsync(ProcessDocuments processDocuments)
        {
            return processDocuments.ProcessDocumentsUsingProducerConsumerPatternChannel();
        }

        

    }

    
}
