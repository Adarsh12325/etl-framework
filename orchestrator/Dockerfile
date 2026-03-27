FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all orchestrator source code
COPY . .

# Write the CSV source file directly into the image.
# Using RUN + printf means the file is always present regardless of
# volume mounts or build-context layout — no external file needed.
RUN mkdir -p /data && printf \
'id,name,department,salary,hire_date\n\
1,Alice Johnson,Engineering,95000.00,2020-03-15\n\
2,Bob Smith,Marketing,72000.00,2019-07-01\n\
3,Carol White,Engineering,105000.00,2018-11-20\n\
4,David Brown,HR,68000.00,2021-01-10\n\
5,Eva Davis,Engineering,98000.00,2020-06-30\n\
6,Frank Wilson,Finance,85000.00,2017-09-05\n\
7,Grace Lee,Marketing,74000.00,2022-02-14\n\
8,Henry Taylor,Engineering,110000.00,2016-04-22\n\
9,Iris Martinez,HR,70000.00,2021-08-19\n\
10,Jack Anderson,Finance,89000.00,2019-12-03\n' \
> /data/source_data.csv

CMD ["python", "main.py"]