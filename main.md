# DE Program

## Intro

---

- Usable raw data for reporting, analytics, and insights.
- Design and build the pipelines that transform and transport data into a usable format.

1. Data sets
2. Pre processing
3. Classification
4. Database
5. Stats
6. Analytics

## Note: dbt - elt’s // fast API // EKS - MD,ROLES,USERS, Command line interface AWS - cli-AWSvoy

## Week 1

---

### Summary

1. Continuous Integration and Containers - Total of 24 hours
2. Overview of Continuous Integration and Containers - 8 hours
3. AWS EKS - 6 hours

- Deliverable: Creating Kubernetes pods for Rail System Ingestion on AWS - 8 hours

### Detail

public dockerHub image

`create deployment <deployment name> image=<imagename> expose deployment <name> —type=<type> —port=<port> scale deployment delete pod autoscale deployment edit deployment set image deployment <deploymentname>=<containername> <imagename> get events —sort-by=<sorter> get componentstatuses get pods get replicaset(s) get deployment(s) get service(s) explain pods get rs describe pod get svc get all delete all rollout history deployment <name> rollout undo deployment <name> diff -f <filename.yaml> apply -f <filename.yaml> logs get hpa top pod top nodes delete hpa <name> create configmap … get configmap`

---

- Docker Overview: `https://www.youtube.com/watch?v=fqMOX6JJhGo`
- Kubernetes: `https://www.youtube.com/watch?v=tqr581_bBM0&list=PLot-YkcC7wZ9xwMzkzR_EkOrPahSofe5Q&index=1`
- aws cli: `https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html`

--

```
# FROM --platform=linux/amd64 python:3.8-slim-buster
FROM python:3.8-slim-buster

WORKDIR /astro

# COPY requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt
# COPY . .

RUN apt-get update
# RUN apt-get upgrade
# RUN curl https://goreleaserdev.blob.core.windows.net/goreleaser-test-container/releases/v1.3.4/cloud-cli_1.3.4_Linux_x86_64.tar.gz -o astrocloudcli.tar.gz
# RUN tar xzf astrocloudcli.tar.gz
# RUN astro dev init
# EXPOSE 5000


COPY /assignment /assignment
# CMD [ "ls -a" ]
# CMD ["python", "hello_test.py"]
```
