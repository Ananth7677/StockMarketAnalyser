from pathlib import Path
from langchain_openai import OpenAI

import pickle

base_dir = Path(__file__).resolve().parent.parent.parent  # This should resolve to MarketWhisper/
pkl_path = base_dir / "cache" / "AAPL_10-K_cache.pkl"

with open(pkl_path, "rb") as f:
    vectorstore = pickle.load(f)

llm = OpenAI(temperature = 0.6)

# qa_pipeline = pipeline("text2text-generation", model="google/flan-t5-small", max_new_tokens=4000)
# llm = HuggingFacePipeline(pipeline=qa_pipeline)

# qa_chain = RetrievalQA.from_chain_type(
#     llm=llm,
#     retriever = vectorstore.as_retriever(),
#     chain_type="stuff",  # or "map_reduce"
#     return_source_documents=True
# )

# query = "how did you decide if the result is good?"
# response = qa_chain.invoke({"query": query})

# print("Answer:", response["result"])