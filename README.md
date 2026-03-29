---
title: Fortress Engine
emoji: 🚀
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
---

# Fortress 2.0 Backend Engine

This repository hosts the FastAPI Quantitative Scanner engine for the Fortress Dashboard.

Deployment: 
- Push strictly the `/engine` contents into a Hugging Face Space set to Docker template.
- Once alive, hook the URL up to the Next.js `NEXT_PUBLIC_API_URL` environment variable.
