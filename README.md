# Finnpanel Data

Päivittäin kerätty arkisto Yle Areenan, MTV Katsomon ja Ruudun katsotuimmista
ohjelmista. Lähde: [Finnpanel](https://www.finnpanel.fi/). Keräys alkoi
**2.9.2024** ja jatkuu automaattisesti.

📊 **[Avaa dashboard](https://jondeman.github.io/Finnpanel-Scraper/)** ·
📖 **[Tekninen dokumentaatio](DOCUMENTATION.md)** ·
💾 **[Tallennussuunnitelma](STORAGE-PLAN.md)**

> Kaikki tämän lohkon ulkopuolinen teksti säilyy — työnkulku päivittää vain
> `BEGIN:generated`-merkkien välisen osan.

<!-- BEGIN:generated -->
_Päivitetty: 2026-08-21 09:37 UTC_

| | |
|---|---|
| Aineisto | 2024-08-30 – 2026-08-21 |
| Päiviä | 719 |
| Rivejä | 86,088 |
| Päivätiedostoja | 1,437 |
| Koottu arkisto | [`data/finnpanel_all.csv`](data/finnpanel_all.csv) (5.8 MB) |

Viimeisimmät tiedostot:
- [`data/2026/14D_Finnpanel_data_2026-08-21.csv`](data/2026/14D_Finnpanel_data_2026-08-21.csv)
- [`data/2026/90D_Finnpanel_data_2026-08-21.csv`](data/2026/90D_Finnpanel_data_2026-08-21.csv)

<!-- END:generated -->

## Aineisto

| Polku | Sisältö |
|---|---|
| [`data/finnpanel_all.csv`](data/finnpanel_all.csv) | **Koko arkisto yhtenä CSV-tiedostona** — tästä kannattaa aloittaa |
| `data/<vuosi>/14D_Finnpanel_data_<pvm>.csv` | Yksi päivä, 14 vrk:n jakso |
| `data/<vuosi>/90D_Finnpanel_data_<pvm>.csv` | Yksi päivä, 90 vrk:n jakso |
| `data/<vuosi>/*.xlsx` | Historia 2.9.2024 – 21.8.2026 alkuperäisessä muodossa |

Kukin päivätiedosto sisältää 60 riviä: kolmen palvelun 20 katsotuinta ohjelmaa
yhdistettynä ja uudelleenjärjestettynä katsojamäärän mukaan.

```python
import pandas as pd
df = pd.read_csv('data/finnpanel_all.csv')
df[df.Period == '14D'].groupby('Service').Viewers.mean()
```

### Sarakkeet

| Sarake | Kuvaus |
|---|---|
| `Date` | Keräyspäivä |
| `Period` | `14D`, `90D` tai `legacy` |
| `Rank` | 1–60, laskettu palvelujen yli |
| `Service` | `Yle Areena`, `MTV Katsomo` tai `Ruutu` |
| `Program` | Ohjelman nimi |
| `Episode` | Jakson nimi — **kerätty vasta 21.8.2026 alkaen** |
| `Duration` | Kesto `H:MM:SS` |
| `Viewers` | Keskikatsojamäärä |
| `PeriodStart` / `PeriodEnd` | Mittausjakso — **kerätty vasta 21.8.2026 alkaen** |

⚠️ `PeriodEnd` on tyypillisesti 1–2 päivää ennen `Date`-arvoa. Aikasarja-analyysissä
käytä `PeriodEnd`-saraketta, älä `Date`-saraketta. Sitä vanhemmille riveille
mittausjaksoa ei valitettavasti voi jälkikäteen palauttaa.

## Ajaminen paikallisesti

```bash
pip install -r requirements.txt

python FPGH.py --dry-run     # hae ja tulosta, älä kirjoita mitään
python FPGH.py               # kirjoita CSV:t hakemistoon data/<vuosi>/
python build_archive.py      # päivitä koottu arkisto
```

Skripti ei tarvitse tunnuksia eikä kirjoita GitHubiin — commitoinnin hoitaa
[työnkulku](.github/workflows/Finnpanel_Scraper.yml).

## Automaatio

Ajastettu joka päivä klo 01:00 UTC. Jos keräys epäonnistuu tai jää vajaaksi,
työnkulku avaa issuen `scraper-failure`-tunnisteella. Finnpanel julkaisee vain
liukuvan ikkunan, joten **menetettyä päivää ei voi kerätä jälkikäteen** — siksi
vajaakin data tallennetaan ja ajo merkitään punaiseksi.

---

Vibe coded by [Joona Kortesmäki](https://www.linkedin.com/in/joonakortesmaki/)
