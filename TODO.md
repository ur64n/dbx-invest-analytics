### TODO:
- AV Sentiment scheduled pipeline wywala się od 2 dni z błędem limitu API.
- Sprawdzić czy bronze.qqq_etf_constituents jest gdziekolwiek używana - jeśli nie, martwa tabela.

---
- Zweryfikowac nazewnictwo tabel i zaktualizowac plik config dev.yaml 
- tabela consistuens jest chyba nie uzywana sprawdzic i dropnac 
- Zmienić metody walidacji emptiness, rows_after_join uniqueness i schema w kazdym pipeline na te z utils i usunąć te niepotrzebne.
- Zaktualizować readme
- Napisać testy jednostkowe | done
- Zmienić w walidacjach .count() na head(), lub count z limitem.
- Sprawdzić we wszystkich plikach #TODO:
- Utworzyć notatniki z EDA dla wszystkich tabel delta w schematach bronze/silver/gold (dołożyć ewentualne walidacje na podstawie wyników wskazujących na anomalie) | in progress
- Stworzyc kod dla retencji danych dla kazdego datasetu
---

Zapytać dla wszystkich metod walidacji i agregacji o fundamentalne mechanizmy tak jak w rozmowie "Widziesz projekt?" <- Można znaleźć ten fragment rozmowy i pociągnąć tam claude podał mechanizm dla agregacji.

Zrobic prompt z prosba o wygenerowania mechanizmow i schematow kolejnosci pisania kodu charakterytycznych dla roznych operacji ekstrakcji/walidcaji/transformacji/enrichmentu/czyszczenia a takze kodu jak laczenie sciezek, zarzadzania plikami, otwierania plikow zamykania zapisu, dopytac o zarzadzanie sparkiem i rozlozeniem/dystrubucja danych opierajac pytania na wiedzy azure z notion dystrybucji danych pomiedzy klastrami i executorami w sparku gdzie sie tym zarzadza w praktyce w kodzie czy w configu czy gdzie jak.