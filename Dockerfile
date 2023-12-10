# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.7.3

# Install additional libraries using pip but as the 'airflow' user
USER airflow
RUN pip install --user \
    numpy \
    requests \ 
    easyocr \
    pytesseract \
    && rm -rf ~/.cache/pip

# Switch back to the root user for any further instructions
USER root
