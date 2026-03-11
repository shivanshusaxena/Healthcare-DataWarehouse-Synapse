# Healthcare Data Warehouse — Azure Synapse Analytics

## Overview
Built a data warehouse on Azure Synapse Analytics (Serverless SQL Pool) 
using a healthcare dataset with 55,500 patient records.

## Architecture
ADLS Gen2 → Azure Synapse Serverless SQL → Star Schema

## Tables Created
- dim_patients — patient demographics
- dim_hospitals — hospital details  
- dim_conditions — medical conditions & medications
- fact_admissions — patient admissions with billing

## Business Questions Answered
1. Which medical condition has highest billing? → Obesity ($25,805 avg)
2. Which hospital admits most patients? → LLC Smith (44 patients)
3. Average length of stay by admission type? → 15 days all types
4. Which age group gets admitted most? → 65+ (16,250 patients)

## Tech Stack
Azure Synapse Analytics, Serverless SQL Pool, ADLS Gen2, 
OPENROWSET, CETAS, Star Schema, SQL

## Dataset
Source: Kaggle — Healthcare Dataset by Prasad Patil
Link: https://www.kaggle.com/datasets/prasad22/healthcare-dataset
55,500 rows | 15 columns
