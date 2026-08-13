FROM python:3.11-slim

WORKDIR /app

# تثبيت dependencies النظام
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# محاولة تثبيت pandas_ta من PyPI، وإذا فشلت من GitHub
RUN pip install --no-cache-dir -r requirements.txt || \
    pip install git+https://github.com/twopirllc/pandas-ta.git

COPY . .

CMD ["python", "main.py"]
