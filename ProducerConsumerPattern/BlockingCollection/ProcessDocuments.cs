using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;


namespace BlockingCollection
{
    public class ProcessDocuments
    {
        public ProcessDocuments()
        {

        }
        private static string[] GetDocumentIdsToProcess()
        {
            string[] documentos = new string[100];
            for (int i = 0; i < documentos.Count(); i++)
            {
                documentos[i] = i.ToString();
            }
            return documentos;
        }
        private static BlockingCollection<string> CreateInputQueue(string[] documentIds)
        {
            var inputQueue = new BlockingCollection<string>();

            foreach (var id in documentIds)
                inputQueue.Add(id);

            inputQueue.CompleteAdding();

            return inputQueue;
        }
        public void Process(string documentId)
        {
            Document document = ReadDocumentFromSourceStore(documentId);

            var translatedDocument = TranslateDocument(document, Language.English);

            SaveDocumentToDestinationStore(translatedDocument);
        }
        Semaphore semaphore = new Semaphore(2, 2);
        public void ProcessSemaphore(string documentId)
        {
            semaphore.WaitOne();

            Document document;

            try
            {
                document = ReadDocumentFromSourceStore(documentId);
            }
            finally
            {
                semaphore.Release();
            }

            var translatedDocument = TranslateDocument(document, Language.English);

            SaveDocumentToDestinationStore(translatedDocument);
        }
        public Document ReadDocumentFromSourceStore(string documentId)
        {
            Thread.Sleep(50);
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Leyendo Documento del disco: {documentId}");
            return new Document(documentId);
        }
        public Document TranslateDocument(Document document, Language lang)
        {
            Thread.Sleep(200);
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"                    Documento Traducido: {document}");
            return new Document(document.DocumentId, lang);
        }
        public Document ReadAndTranslateDocument(string documentId)
        {
            var documento = ReadDocumentFromSourceStore(documentId);
            return TranslateDocument(documento, Language.English);
        }
        public void SaveDocumentToDestinationStore(Document translatedDocument)
        {
            Thread.Sleep(100);
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"                                        Documento traducido guardado en el disco: {translatedDocument}");
        }

        public void ProcessDocumentsUsingNoobsterMind()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            foreach (var documentId in documentIds)
            {
                Process(documentId);
            }
        }
        public void ProcessDocumentsInParallel()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            Parallel.ForEach(
                documentIds,
                id => Process(id));
        }
        public void ProcessDocumentsInParallelSemaphore()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            Parallel.ForEach(
                documentIds,
                id => ProcessSemaphore(id));
        }
        public void ProcessDocumentsUsingProducerConsumerPattern()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            BlockingCollection<string> inputQueue = CreateInputQueue(documentIds);

            BlockingCollection<Document> queue = new BlockingCollection<Document>(500);

            var consumer = Task.Run(() =>
            {
                foreach (var translatedDocument in queue.GetConsumingEnumerable())
                {
                    SaveDocumentToDestinationStore(translatedDocument);
                }
            });

            var producers = Enumerable.Range(0, 7)
                .Select(_ => Task.Run(() =>
                {
                    foreach (var documentId in inputQueue.GetConsumingEnumerable())
                    {
                        var document = ReadAndTranslateDocument(documentId);
                        queue.Add(document);
                    }
                }))
                .ToArray();

            Task.WaitAll(producers);
            Console.WriteLine($"Producer ha terminado");

            queue.CompleteAdding();

            consumer.Wait();
            Console.WriteLine($"Consumer ha terminado");

        }
        public void ProcessDocumentsUsingPipelinePattern()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            BlockingCollection<string> inputQueue = CreateInputQueue(documentIds);

            BlockingCollection<Document> queue1 = new BlockingCollection<Document>(500);

            BlockingCollection<Document> queue2 = new BlockingCollection<Document>(500);

            var savingTask = Task.Run(() =>
            {
                foreach (var translatedDocument in queue2.GetConsumingEnumerable())
                {
                    SaveDocumentToDestinationStore(translatedDocument);
                }
            });

            var translationTasks =
                Enumerable.Range(0, 7)
                    .Select(_ =>
                        Task.Run(() =>
                        {
                            foreach (var readDocument in queue1.GetConsumingEnumerable())
                            {
                                var translatedDocument =
                                    TranslateDocument(readDocument, Language.English);

                                queue2.Add(translatedDocument);
                            }
                        }))
                    .ToArray();

            var readingTasks =
                Enumerable.Range(0, 4)
                    .Select(_ =>
                        Task.Run(() =>
                        {
                            foreach (var documentId in inputQueue.GetConsumingEnumerable())
                            {
                                var document = ReadDocumentFromSourceStore(documentId);

                                queue1.Add(document);
                            }
                        }))
                    .ToArray();

            Task.WaitAll(readingTasks);

            queue1.CompleteAdding();
            Console.WriteLine("Lectura completada");

            Task.WaitAll(translationTasks);

            queue2.CompleteAdding();
            Console.WriteLine("Guardado completada");

            savingTask.Wait();
        }

    }
    public class Document
    {
        public Document(string documentId)
        {
            DocumentId = documentId;
        }
        public Document(string documentId, Language language)
        {
            DocumentId = documentId;
            language = Language;
        }
        public string DocumentId { get; set; }
        public Language Language{ get; set; }
        public override string ToString()
        {
            return $"Doc #{DocumentId}";
        }
    }
    public enum Language
    {
        English,
        Spanish,
        German
    }
}
