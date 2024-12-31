# Airbyte-Hackathon-GGG
The Airbyte+Motherduck hackathon project by the Green Grid Geeks team. 


## The Project 
A data app that tracks the energy economy in finland. This will be accomplished by 
querying data from [Fingrid](https://data.fingrid.fi/en/datasets).
The data will be accessed in 15min intervals from the API using Airbyte, 
the data will be stored in a Motherduck dwh and presented using a Streamlit app. 

## The Tech Stack

[INSERT DIAGRAM HERE]

#### Ingestion
- Airbyte

#### Storage
- Motherduck

#### Business Intelligence
- Streamlit

#### Infrastructure 
- Terraform

## Development

Instructions for setting up the project:

### Dependencies

Dependencies are managed by [uv](https://docs.astral.sh/uv/).
To install dependencies run:
```shell
uv sync
```

### Database connection

First [generate an access token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/) in MotherDuck.
Store the token in a `.env` file as `MOTHERDUCK_TOKEN`.

## The Team
 - Zharko Cekovski aka Zesky665
 - Jonathan Biemond aka jonbiemond
 - Ronobir Das aka Data4Dayz
