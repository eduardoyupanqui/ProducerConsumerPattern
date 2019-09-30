using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using ProceduralDataflow;
using System.Threading.Channels;
using System.Diagnostics;

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
        Random random = new Random();
        public Document ReadDocumentFromSourceStore(string documentId)
        {
            Thread.Sleep(50);
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Leyendo Documento del disco: {documentId}");
            return new Document(documentId, (Language)random.Next(2, 3));
        }
        public Document TranslateDocument(Document document, Language languageDestino)
        {
            Thread.Sleep(200);
            //Translation from lang to English
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"                    Documento Traducido: {document}");
            return new Document(document.DocumentId, languageDestino);
        }
        public Document TranslateSpanishDocument(Document document, Language languageDestino) {
            return TranslateDocument(document, languageDestino);
        }
        public Document TranslateGermanDocument(Document document, Language languageDestino)
        {
            return TranslateDocument(document, languageDestino);
        }
        public async Task<Document> TranslateGermanDocumentAsync(Document document, Language languageGerman)
        {
            return await Task.Run(()=>TranslateDocument(document, languageGerman));
        }
        public Document ReadAndTranslateDocument(string documentId)
        {
            var documento = ReadDocumentFromSourceStore(documentId);
            return TranslateDocument(documento, Language.English);
        }
        public Task<Document> ReadAndTranslateDocumentAsync(string documentId) {
            return Task.Run(()=> ReadAndTranslateDocument(documentId));
        }

        public void SaveDocumentToDestinationStore(Document translatedDocument)
        {
            Thread.Sleep(100);
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"                                        Documento traducido guardado en el disco: {translatedDocument}");
        }
        public Task SaveDocumentToDestinationStoreAsync(Document translatedDocument) {
            return Task.Run(() => SaveDocumentToDestinationStore(translatedDocument));
        }
        public Language GetDocumentLanguage(Document document) {
            return document.Language;
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

        public void ProcessDocumentsUsingDataflowPattern()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            BlockingCollection<string> inputQueue = CreateInputQueue(documentIds);

            BlockingCollection<Document> spanishDocumentsQueue = new BlockingCollection<Document>(10);

            BlockingCollection<Document> germanDocumentsQueue = new BlockingCollection<Document>(100);

            BlockingCollection<Document> translatedDocumentsQueue = new BlockingCollection<Document>(100);


            var readingTask =
                Task.Run(() =>
                {
                    foreach (var documentId in inputQueue.GetConsumingEnumerable())
                    {
                        var document = ReadDocumentFromSourceStore(documentId);

                        var language = GetDocumentLanguage(document);

                        if (language == Language.Spanish)
                            spanishDocumentsQueue.Add(document);
                        else if (language == Language.German)
                            germanDocumentsQueue.Add(document);
                    }
                });

            var spanishTranslationTask =
                Task.Run(() =>
                {
                    foreach (var readDocument in spanishDocumentsQueue.GetConsumingEnumerable())
                    {
                        var translatedDocument =
                            TranslateSpanishDocument(readDocument, Language.English);

                        translatedDocumentsQueue.Add(translatedDocument);
                    }
                });

            var germanTranslationTask =
                Task.Run(async () =>
                {
                    foreach (var readDocument in germanDocumentsQueue.GetConsumingEnumerable())
                    {
                        var translatedDocument =
                            await TranslateGermanDocumentAsync(readDocument, Language.English);

                        translatedDocumentsQueue.Add(translatedDocument);
                    }
                });

            var savingTask = Task.Run(() =>
            {
                foreach (var translatedDocument in translatedDocumentsQueue.GetConsumingEnumerable())
                {
                    SaveDocumentToDestinationStore(translatedDocument);
                }
            });

            readingTask.Wait();

            spanishDocumentsQueue.CompleteAdding();

            germanDocumentsQueue.CompleteAdding();

            spanishTranslationTask.Wait();

            germanTranslationTask.Wait();

            translatedDocumentsQueue.CompleteAdding();

            savingTask.Wait();
        }
        public void ProcessDocumentsUsingTplDataflow()
        {
            var readBlock =
                new TransformBlock<string, Document>(
                    x => ReadDocumentFromSourceStore(x),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 }); //1

            var spanishDocumentTranslationBlock =
                new TransformBlock<Document, Document>(
                    x => TranslateSpanishDocument(x, Language.English),
                    new ExecutionDataflowBlockOptions { BoundedCapacity = 10 }); //2

            var germanDocumentTranslationBlock =
                new TransformBlock<Document, Document>(
                    x => TranslateGermanDocumentAsync(x, Language.English),
                    new ExecutionDataflowBlockOptions { BoundedCapacity = 100 }); //3

            var saveDocumentsBlock =
                new ActionBlock<Document>(
                    x => SaveDocumentToDestinationStore(x),
                    new ExecutionDataflowBlockOptions { BoundedCapacity = 100 }); //4

            readBlock.LinkTo(
                spanishDocumentTranslationBlock,
                new DataflowLinkOptions { PropagateCompletion = true },
                x => GetDocumentLanguage(x) == Language.Spanish); //5

            readBlock.LinkTo(
                germanDocumentTranslationBlock,
                new DataflowLinkOptions { PropagateCompletion = true },
                x => GetDocumentLanguage(x) == Language.German); //6

            spanishDocumentTranslationBlock.LinkTo(
                saveDocumentsBlock); //7

            germanDocumentTranslationBlock.LinkTo(
                saveDocumentsBlock); //8

            string[] documentIds = GetDocumentIdsToProcess(); //9

            foreach (var id in documentIds)
                readBlock.Post(id); //10

            Task.WhenAll(
                    spanishDocumentTranslationBlock.Completion,
                    germanDocumentTranslationBlock.Completion)
                .ContinueWith(_ => saveDocumentsBlock.Complete()); //11

            readBlock.Complete(); //12

            saveDocumentsBlock.Completion.Wait(); //13
        }
        public void ProcessDocumentsUsingParallelForEach()
        {
            string[] documentIds = GetDocumentIdsToProcess();

            Parallel.ForEach(documentIds, documentId =>
            {
                var document = ReadDocumentFromSourceStore(documentId);

                var language = GetDocumentLanguage(document);

                Document translatedDocument;

                if (language == Language.Spanish)
                    translatedDocument = TranslateSpanishDocument(document, Language.English);
                else // if (language == Language.German)
                    translatedDocument = TranslateGermanDocument(document, Language.English);

                SaveDocumentToDestinationStore(translatedDocument);
            });
        }
        public void ProcessDocumentsUsingProceduralDataflow()
        {
            var readDocumentsBlock =
                ProcDataflowBlock.StartDefault(
                    maximumNumberOfActionsInQueue: 100,
                    maximumDegreeOfParallelism: 1);

            var spanishDocumentsTranslationBlock =
                ProcDataflowBlock.StartDefault(
                    maximumNumberOfActionsInQueue: 10,
                    maximumDegreeOfParallelism: 1);

            var germanDocumentsTranslationBlock =
                AsyncProcDataflowBlock.StartDefault(
                    maximumNumberOfActionsInQueue: 100,
                    maximumDegreeOfParallelism: 1);

            var saveDocumentsBlock =
                ProcDataflowBlock.StartDefault(
                    maximumNumberOfActionsInQueue: 100,
                    maximumDegreeOfParallelism: 1);

            DfTask<Document> DfReadDocumentFromSourceStore(
                string documentId) =>
                readDocumentsBlock.Run(
                    () => ReadDocumentFromSourceStore(documentId));

            DfTask<Document> DfTranslateSpanishDocument(
                Document document,
                Language destinationLanguage) =>
                spanishDocumentsTranslationBlock.Run(
                    () => TranslateSpanishDocument(document, destinationLanguage));

            DfTask<Document> DfTranslateGermanDocument(
                Document document,
                Language destinationLanguage) =>
                germanDocumentsTranslationBlock.Run(
                    () => TranslateGermanDocumentAsync(document, destinationLanguage));

            DfTask DfSaveDocumentToDestinationStore(
                Document document) =>
                saveDocumentsBlock.Run(
                    () => SaveDocumentToDestinationStore(document));

            async Task ProcessDocument(string documentId)
            {
                var document = await DfReadDocumentFromSourceStore(documentId);

                var language = GetDocumentLanguage(document);

                Document translatedDocument;

                if (language == Language.Spanish)
                    translatedDocument =
                        await DfTranslateSpanishDocument(document, Language.English);
                else // if (language == Language.German)
                    translatedDocument =
                        await DfTranslateGermanDocument(document, Language.English);

                await DfSaveDocumentToDestinationStore(translatedDocument);
            }

            string[] documentIds = GetDocumentIdsToProcess();

            var processingTask =
                EnumerableProcessor.ProcessEnumerable(
                    documentIds,
                    documentId => ProcessDocument(documentId),
                    maximumNumberOfNotCompletedTasks: 100);

            processingTask.Wait();
        }


        //async Task ProcessDocument(string documentId)
        //{
        //    var document = await DfReadDocumentFromSourceStore(documentId);

        //    var language = GetDocumentLanguage(document);

        //    Document translatedDocument;

        //    try
        //    {
        //        if (language == Language.Spanish)
        //            translatedDocument =
        //                await DfTranslateSpanishDocument(document, Language.English);
        //        else // if (language == Language.German)
        //            translatedDocument =
        //                await DfTranslateGermanDocument(document, Language.English);
        //    }
        //    catch (Exception ex)
        //    {
        //        await DfStoreDocumentInFaultedDocumentsStore(documentId, document, ex);
        //        return;
        //    }

        //    await DfSaveDocumentToDestinationStore(translatedDocument);
        //}

        //async Task ProcessDocument(string documentId)
        //{
        //    var englishDocument = await DfReadDocumentFromSourceStore(documentId);

        //    DfTask<Document> toSpanishTask =
        //        DfTranslateEnglishDocumentToSpanish(englishDocument);

        //    DfTask<Document> toGermanTask =
        //        DfTranslateEnglishDocumentToGerman(englishDocument);

        //    SpanishAndGermanDocuments documents =
        //        await DfTask.WhenAll(
        //            toSpanishTask,
        //            toGermanTask,
        //            (spanish, german) =>
        //                new SpanishAndGermanDocuments(spanish, german));

        //    await DfCompressAndSaveDocuments(documents);
        //}


        public async Task ProcessDocumentsUsingProducerConsumerPatternChannel()
        {
            const int boundedQueueSize = 500;
            var savingChannel = Channel.CreateBounded<Document>(boundedQueueSize);
            var translationChannel = Channel.CreateUnbounded<string>();

            var saveDocumentTask = Task.Run(async () =>
            {
                while (await savingChannel.Reader.WaitToReadAsync())
                {
                    while (savingChannel.Reader.TryRead(out var document))
                    {
                        Debug.WriteLine($"Saving document: {document}");
                        await SaveDocumentToDestinationStoreAsync(document);
                    }
                }
            });

            var translateDocumentTasks = Enumerable
                .Range(0, 7) // 7 translation tasks
                .Select(_ => Task.Run(async () =>
                {
                    while (await translationChannel.Reader.WaitToReadAsync())
                    {
                        while (translationChannel.Reader.TryRead(out var documentId))
                        {
                            Debug.WriteLine($"Reading and translating document {documentId}");
                            var document = await ReadAndTranslateDocumentAsync(documentId);
                            await savingChannel.Writer.WriteAsync(document);
                        }
                    }
                }))
                .ToArray();

            var allItemsAreWrittenIntoTranslationChannel = GetDocumentIdsToProcess()
                                                            .Select(id => translationChannel.Writer.TryWrite(id))
                                                            .All(_ => _);
            Debug.Assert(allItemsAreWrittenIntoTranslationChannel); // All items should be written successfully into the unbounded channel.translationChannel.Writer.Complete();

            await Task.WhenAll(translateDocumentTasks);
            savingChannel.Writer.Complete();

            await savingChannel.Reader.Completion;
        }

    }
    public class Document
    {
        public Document(string documentId, Language language)
        {
            DocumentId = documentId;
            Language = language;
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
        English = 1,
        Spanish = 2,
        German = 3
    }
}
