FROM python:3.12
WORKDIR /app
COPY REQUIRE.txt /app/REQUIRE.txt
RUN pip install -r /app/REQUIRE.txt
COPY . /app/
CMD ["python", "-m", "runners.liveness_checker"]