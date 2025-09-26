# Ohjelmointitehtävä

## Tehtävän kuvaus

Toteuta ohjelma, joka:

1. **Lukee** asiakas-, tuote-, varasto- ja tilaustiedot CSV-tiedostoista ja tallentaa ne SQLite-tietokantaan (skeema tiedostossa `db-schema.sql`).
2. **Lukee** ALV-säännöt JSON-tiedostosta (ei tallenneta kantaan, vaan käytetään laskennan aikana).
3. **Laskee** jokaiselle tilaukselle:
   - nettosumman
   - ALV:n (käyttäen tuotteiden ALV-koodeja ja JSON-sääntöjä)
   - bruttosumman
   - tiedon, onko tilaus täysin varastossa (Ei tarvitse välittää muista tilauksista)
4. **Tuottaa** raportin tilauksista CSV-muodossa:
   - `order_totals.csv`: `order_id, customer_name, net_total, vat_total, gross_total, is_fully_in_stock`

Ohjelmointikielen voi valita seuraavista: Python, PHP, Javascript ja Ruby.

## Lähtödata

Saat harjoituksen mukana seuraavat tiedostot:

- `customers.csv`
- `products.csv`
- `stock_levels.csv`
- `orders.csv`
- `order_lines.csv`
- `tax_rules.json`
