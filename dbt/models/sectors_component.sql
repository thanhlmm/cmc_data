{% set sector_components = ["Lending & Borrowing","Web3","DeFi Index","Identity","Oracles","Yearn Partnerships","Derivatives","Asset Management","DeFi 2.0","Memes","Play To Earn","Yield Aggregator","Yield Farming","Stablecoin","Fan Token","Wrapped Tokens","DAO","AMM","Filesharing","Protocol-Owned Liquidity","Interoperability","Synthetics","Tokenized Stock","Metaverse"] %}

SELECT
  timestamp,
  {% for component in sector_components %}
    SUM(marketcap) FILTER (WHERE name = '{{component}}') AS "{{component}}_marketCap",
    SUM(marketVolume) FILTER (WHERE name = '{{component}}') AS "{{component}}_marketVolume",
  {% endfor %}
  sum(marketcap) as totalMarketCap
FROM {{ ref('sectors') }} 
GROUP BY timestamp
ORDER BY timestamp
