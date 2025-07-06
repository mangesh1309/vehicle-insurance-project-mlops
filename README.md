# 🚗 Vehicle Insurance Claim Prediction – End-to-End MLOps Project

This project demonstrates an end-to-end **MLOps pipeline** for a machine learning system that predicts whether a customer will file a vehicle insurance claim. The goal is to showcase how to structure, build, and deploy a real-world machine learning system using modern tools and DevOps practices.

> ✨ Designed to impress recruiters by demonstrating hands-on skills with **Docker**, **CI/CD**, **AWS**, **MongoDB**, and **Machine Learning**.

---

## 📌 Project Highlights

✅ End-to-End ML Pipeline (Data → Model → App)  
✅ Scalable & Reproducible Codebase  
✅ CI/CD Integration with GitHub Actions  
✅ Dockerized Application  
✅ Deployed on AWS EC2 with S3 Model Registry  
✅ MongoDB Atlas as Cloud DB

---

## 🧱 Project Structure

```
vehicle-insurance-mlops/
├── .github/workflows/         # GitHub Actions for CI/CD
├── artifact/                  # Model artifacts and logs
├── components/                # Data ingestion, validation, transformation
├── configuration/            # DB and AWS connection logic
├── constants/                # Global constants and schema
├── entity/                   # Config and artifacts classes
├── notebook/                 # Jupyter notebooks and MongoDB demo
├── static/, templates/       # Flask frontend
├── app.py                    # Flask app
├── Dockerfile, docker-compose.yml
├── requirements.txt
└── setup.py, pyproject.toml
```

---

## 🧠 Key Technologies & Services

| Category           | Tools & Services                         |
|--------------------|------------------------------------------|
| ML & Data          | Pandas, Sklearn, PyYAML, Jupyter Notebook |
| Data Storage       | **MongoDB Atlas** (Cloud NoSQL Database) |
| Deployment         | **Docker**, **AWS EC2**, **ECR**, **S3** |
| Orchestration      | **GitHub Actions** (CI/CD), Shell scripts |
| DevOps Practices   | Environment Variables, Secrets, Logging  |

---

## ⚙️ Setup Guide

### 🔹 1. Create Template & Install Packages
```bash
python template.py
conda create -n vehicle python=3.10 -y
conda activate vehicle
pip install -r requirements.txt
```

### 🔹 2. MongoDB Atlas Setup
- Create a free cluster (M0)
- Add `0.0.0.0/0` to network access
- Get connection string (Python > v3.6)
- Store as `MONGODB_URL` environment variable

### 🔹 3. Push Dataset to MongoDB
- Use `mongoDB_demo.ipynb` inside `notebook/`
- Browse data in MongoDB → Database → Collections

---

## 📥 Data Ingestion & Validation

- Declare configs in `constants/__init__.py`
- DB logic: `configuration/mongo_db_connections.py`
- Ingestion logic: `data_access/proj1_data.py`
- Pipeline config: `entity/config_entity.py`, `artifact_entity.py`
- Run: `demo.py`

### 🔸 Data Validation
- Define schema in `config/schema.yaml`
- Add utility methods in `utils/main_utils.py`
- Validate using component logic in `components/data_validation.py`

---

## 🔄 Data Transformation & Model Training

- Transformation logic includes feature scaling & encoding
- Model is trained using `RandomForest`, `LogisticRegression`, etc.
- Training pipeline is modular and reproducible

---

## 📊 Model Evaluation & AWS S3 Integration

- Create S3 Bucket `my-model-mlopsproj`
- IAM user with `AdministratorAccess`
- Upload trained models to S3 using `aws_storage/` & `s3_estimator.py`
- Eval logic uses:
```python
MODEL_EVALUATION_CHANGED_THRESHOLD_SCORE = 0.02
MODEL_PUSHER_S3_KEY = "model-registry"
```

---

## 🚀 CI/CD Pipeline

### 🐳 Dockerize App
```dockerfile
FROM python:3.10
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "app.py"]
```

### 🔁 GitHub Actions
`.github/workflows/aws.yml`  
Triggers on `push` to `main`:
- Builds & pushes Docker image to **ECR**
- Triggers self-hosted runner on **EC2**
- Pulls latest image & runs container

### ☁️ EC2 Setup
- Ubuntu 24.04 server
- Installed Docker
- Registered as GitHub Self-Hosted Runner
- Port `5080` opened for app access

---

## 🔐 GitHub Secrets Configured

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`
- `ECR_REPO`
- `MONGODB_URL`

---

## 🌐 Application Access

After deployment, access your Flask app via:

```
http://<your-ec2-public-ip>:5080
```

Training route (optional):  
```
http://<your-ec2-public-ip>:5080/training
```

---

## 🎯 Final Thoughts

This MLOps project brings together the **best practices** in:
- Machine Learning Engineering
- Cloud Deployment
- DevOps Automation

> Use this project to showcase your ability to build and deploy production-ready ML applications from scratch.

---
