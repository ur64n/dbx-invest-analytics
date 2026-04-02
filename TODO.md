### TODO:
- AV Sentiment scheduled pipeline wywala się od 2 dni z błędem limitu API.
- Sprawdzić czy bronze.qqq_etf_constituents jest gdziekolwiek używana - jeśli nie, martwa tabela.

- Zmienić metody walidacji emptiness, rows_after_join uniqueness i schema w kazdym pipeline na te z utils i usunąć te niepotrzebne.
- Zaktualizować readme
- Napisać testy jednostkowe dla nowego pajpa i ekstrakcji jesli potrzebne sentiment history
- Zmienić w walidacjach .count() na head(), lub count z limitem.
- Sprawdzić we wszystkich plikach #TODO:
- Stworzyc kod dla retencji danych dla kazdego datasetu
---

Zapytać dla wszystkich metod walidacji i agregacji o fundamentalne mechanizmy tak jak w rozmowie "Widziesz projekt?" <- Można znaleźć ten fragment rozmowy i pociągnąć tam claude podał mechanizm dla agregacji.

Zrobic prompt z prosba o wygenerowania mechanizmow i schematow kolejnosci pisania kodu charakterytycznych dla roznych operacji ekstrakcji/walidcaji/transformacji/enrichmentu/czyszczenia a takze kodu jak laczenie sciezek, zarzadzania plikami, otwierania plikow zamykania zapisu, dopytac o zarzadzanie sparkiem i rozlozeniem/dystrubucja danych opierajac pytania na wiedzy azure z notion dystrybucji danych pomiedzy klastrami i executorami w sparku gdzie sie tym zarzadza w praktyce w kodzie czy w configu czy gdzie jak.