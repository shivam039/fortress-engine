---
title: Fortress Engine
emoji: 🚀
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
---

# Fortress 2.0 Backend Engine

This repository hosts the FastAPI Quantitative Scanner engine that powers the Fortress Streamlit dashboard.

Deployment: 
- Push strictly the `/engine` contents into a Hugging Face Space set to Docker template.
- Once alive, point the Streamlit app to the deployed API with `FORTRESS_API_URL`.
