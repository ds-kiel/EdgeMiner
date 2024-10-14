FROM pm4py/pm4py-core:2.7.11
WORKDIR /application

RUN pip install bottle==0.12.25 requests paste pandas

COPY ./ /application/

# CMD ["python", "-u", "./activity_node.py"]
CMD ["python", "-u", "./improved_activity_node.py"]
