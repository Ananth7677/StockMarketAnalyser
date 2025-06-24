import json
import re
from io import StringIO

import nltk
import pandas as pd
from bs4 import BeautifulSoup
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from transformers import AutoTokenizer

# Initialize NLTK
nltk.download('punkt', quiet=True)


# Initialize tokenizer and embedding model
tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-base")
embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

def clean_table_html(table):
    """
    Clean HTML table content to improve parsing by pandas.read_html.
    
    Args:
        table (BeautifulSoup): Table element.
    
    Returns:
        str: Cleaned HTML string.
    """
    # Remove nested tables, scripts, and styles
    for tag in table.find_all(['table', 'script', 'style']):
        tag.decompose()
    # Normalize whitespace and remove non-standard attributes
    html = str(table)
    html = re.sub(r'\s+', ' ', html)
    html = re.sub(r'(<[^>]+?)\s+[^>]*?(>)', r'\1\2', html)  # Clean attributes
    return html

def chunk_text_by_sentence(full_text, soup_doc, max_tokens=256, overlap_sentences=2):
    """
    Chunk narrative text by sentences, removing table content.
    
    Args:
        full_text (str): Cleaned text from clean_content.
        soup_doc (BeautifulSoup): Parsed HTML document.
        max_tokens (int): Max tokens per chunk (default: 256 for all-MiniLM-L6-v2).
        overlap_sentences (int): Sentences to overlap (default: 2).
    
    Returns:
        list: Text chunks as strings.
    """
    # Remove table content from full_text
    tables = soup_doc.find_all("table")
    for table in tables:
        table_text = " ".join(table.get_text(strip=True).split())
        full_text = full_text.replace(table_text, "")
    
    # Split into sentences
    sentences = nltk.sent_tokenize(full_text.strip())
    text_chunks = []
    current_chunk = []
    current_tokens = 0
    
    for sentence in sentences:
        sentence_tokens = len(tokenizer.encode(sentence, add_special_tokens=True))
        
        # If adding sentence exceeds max_tokens, finalize chunk
        if current_tokens + sentence_tokens > max_tokens and current_chunk:
            chunk = " ".join(current_chunk)
            chunk_tokens = len(tokenizer.encode(chunk, add_special_tokens=True))
            if chunk_tokens <= max_tokens:
                text_chunks.append(chunk)
            else:
                print(f"Warning: Oversized text chunk ({chunk_tokens} tokens), splitting")
                text_chunks.extend(split_oversized_chunk(chunk, max_tokens))
            
            # Overlap: take last overlap_sentences
            current_chunk = current_chunk[-overlap_sentences:] if len(current_chunk) >= overlap_sentences else current_chunk
            current_tokens = len(tokenizer.encode(" ".join(current_chunk), add_special_tokens=True))
        
        current_chunk.append(sentence)
        current_tokens += sentence_tokens
    
    # Add final chunk
    if current_chunk:
        chunk = " ".join(current_chunk)
        chunk_tokens = len(tokenizer.encode(chunk, add_special_tokens=True))
        if chunk_tokens <= max_tokens:
            text_chunks.append(chunk)
        else:
            print(f"Warning: Oversized final text chunk ({chunk_tokens} tokens), splitting")
            text_chunks.extend(split_oversized_chunk(chunk, max_tokens))
    
    return text_chunks

def split_oversized_chunk(chunk, max_tokens):
    """
    Split an oversized chunk into smaller chunks.
    
    Args:
        chunk (str): Oversized chunk text.
        max_tokens (int): Max tokens per chunk.
    
    Returns:
        list: Smaller chunks.
    """
    sentences = nltk.sent_tokenize(chunk.strip())
    sub_chunks = []
    current_sub_chunk = []
    current_tokens = 0
    
    for sentence in sentences:
        sentence_tokens = len(tokenizer.encode(sentence, add_special_tokens=True))
        if current_tokens + sentence_tokens > max_tokens and current_sub_chunk:
            sub_chunks.append(" ".join(current_sub_chunk))
            current_sub_chunk = []
            current_tokens = 0
        current_sub_chunk.append(sentence)
        current_tokens += sentence_tokens
    
    if current_sub_chunk:
        sub_chunks.append(" ".join(current_sub_chunk))
    
    return sub_chunks

def chunk_tables(tables, max_tokens=256):
    """
    Chunk tables into JSON strings under max_tokens.
    
    Args:
        tables (list): BeautifulSoup table elements.
        max_tokens (int): Max tokens per chunk (default: 256).
    
    Returns:
        list: Table chunks as JSON strings.
    """
    table_chunks = []
    for i, table in enumerate(tables):
        try:
            # Clean table HTML
            cleaned_html = clean_table_html(table)
            df = pd.read_html(StringIO(cleaned_html), flavor='lxml')[0]
            df = df.fillna("")
            df.columns = [str(col).replace("\n", " ").strip() for col in df.columns]
            
            # Estimate rows per chunk
            sample_row = json.dumps({
                "table": f"Table_{i+1}",
                "rows": "1-1",
                "columns": list(df.columns),
                "data": [df.iloc[0].to_dict()],
                "note": "From SEC filing, values may be in millions USD"
            })
            tokens_per_row = len(tokenizer.encode(sample_row, add_special_tokens=True)) / max(1, len(df.columns))
            rows_per_chunk = max(1, int(max_tokens / tokens_per_row / 2))  # Conservative
            
            for start in range(0, len(df), rows_per_chunk):
                chunk_df = df.iloc[start:start + rows_per_chunk]
                table_chunk = {
                    "table": f"Table_{i+1}",
                    "rows": f"{start+1}-{min(start + rows_per_chunk, len(df))}",
                    "columns": list(chunk_df.columns),
                    "data": chunk_df.to_dict(orient="records"),
                    "note": "From SEC filing, values may be in millions USD"
                }
                chunk_json = json.dumps(table_chunk)
                chunk_tokens = len(tokenizer.encode(chunk_json, add_special_tokens=True))
                if chunk_tokens <= max_tokens:
                    table_chunks.append(chunk_json)
                else:
                    print(f"Warning: Oversized table chunk {i+1} ({chunk_tokens} tokens), reducing rows")
                    smaller_rows = max(1, rows_per_chunk // 2)
                    if smaller_rows < rows_per_chunk:
                        chunk_df = df.iloc[start:start + smaller_rows]
                        table_chunk = {
                            "table": f"Table_{i+1}",
                            "rows": f"{start+1}-{min(start + smaller_rows, len(df))}",
                            "columns": list(chunk_df.columns),
                            "data": chunk_df.to_dict(orient="records"),
                            "note": "From SEC filing, values may be in millions USD"
                        }
                        table_chunks.append(json.dumps(table_chunk))
        except Exception as e:
            print(f"Error processing table {i+1}: {e}")
            # Fallback: Extract table as text
            table_text = " ".join(table.get_text(strip=True).split())
            if table_text:
                text_chunks = split_oversized_chunk(table_text, max_tokens)
                table_chunks.extend([f"Table_{i+1} (text fallback Tamarind): {chunk}" for chunk in text_chunks])
            continue
    return table_chunks

def validate_chunks(text_chunks, table_chunks, max_tokens=256):
    """
    Validate and combine chunks, splitting oversized ones.
    
    Args:
        text_chunks (list): Text chunks.
        table_chunks (list): Table chunks.
 unexpectedly long chunk
        max_tokens (int): Max tokens per chunk (default: 256).
    
    Returns:
        list: Validated chunks.
    """
    all_chunks = text_chunks + table_chunks
    validated_chunks = []
    
    for i, chunk in enumerate(all_chunks):
        tokens = len(tokenizer.encode(chunk, add_special_tokens=True))
        if tokens > max_tokens:
            print(f"Warning: Chunk {i+1} exceeds {max_tokens} tokens ({tokens} tokens), splitting")
            if chunk.startswith("{"):  # Table chunk (JSON)
                try:
                    table_data = json.loads(chunk)
                    df = pd.DataFrame(table_data["data"], columns=table_data["columns"])
                    mid = len(df) // 2
                    for start, end in [(0, mid), (mid, len(df))]:
                        if start < end:
                            sub_chunk = {
                                "table": table_data["table"],
                                "rows": f"{start+1}-{end}",
                                "columns": table_data["columns"],
                                "data": df.iloc[start:end].to_dict(orient="records"),
                                "note": table_data["note"]
                            }
                            validated_chunks.append(json.dumps(sub_chunk))
                except Exception as e:
                    print(f"Error splitting table chunk {i+1}: {e}")
                    continue
            else:  # Text chunk
                validated_chunks.extend(split_oversized_chunk(chunk, max_tokens))
        else:
            validated_chunks.append(chunk)
            print(f"Chunk {i+1}: {tokens} tokens, {len(chunk)} characters")
    
    return validated_chunks

def embed_and_vector_store(chunks):
    """
    Embed chunks and create FAISS vector store.
    
    Args:
        chunks (list): Text and table chunks.
    
    Returns:
        FAISS: Vector store.
    """
    try:
        vectorstore = FAISS.from_texts(texts=chunks, embedding=embedding_model)
        return vectorstore
    except Exception as e:
        print(f"Error creating vector store: {e}")
        return None