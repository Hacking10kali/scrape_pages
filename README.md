# Anime-Sama Scraper

## Structure du repo
```
.
├── scraper.py                     ← script principal
├── .github/workflows/scrape.yml   ← workflow GitHub Actions
└── AnimeData/
    ├── page_1.json
    ├── page_2.json
    └── ...
```

## Lancement

1. Va dans l'onglet **Actions** de ton repo GitHub
2. Clique sur **Anime Sama Scraper** dans la liste à gauche
3. Clique sur **Run workflow**
4. Remplis :
   - `Première page` : ex. `1`
   - `Dernière page`  : ex. `43`
5. Clique **Run workflow** → les JSON sont committés automatiquement

## Variables d'environnement (optionnel)

Tu peux ajuster les workers dans le fichier `.github/workflows/scrape.yml` :

| Variable            | Défaut | Description                    |
|---------------------|--------|--------------------------------|
| MAX_PAGE_WORKERS    | 2      | Pages en parallèle             |
| MAX_ANIME_WORKERS   | 4      | Animés en parallèle par page   |
| MAX_SAISON_WORKERS  | 3      | Saisons en parallèle par anime |
| MAX_EPISODE_WORKERS | 5      | Épisodes en parallèle          |

## Découper en plusieurs jobs (si timeout)

Pour 43 pages tu peux lancer plusieurs fois :
- Run 1 : pages 1 → 15
- Run 2 : pages 16 → 30
- Run 3 : pages 31 → 43
