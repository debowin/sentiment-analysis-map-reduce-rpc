service SentimentAnalyzerService {
    bool ping(),
    string getSentiments(1: list<string> fileNames),
    void returnSentimentResult(1:string fileName),
}

service ComputeService {
    bool ping(),
    bool mapTask(1: string fileName),
    string sortTask(1: list<string> fileNames),
}