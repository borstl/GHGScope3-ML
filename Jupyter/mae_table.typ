table(
  columns: (3fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr),
  inset: 4pt,
  align: (left + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon, center + horizon),
  stroke: 0.4pt + rgb("#d8cde2"),

  table.header([Emissionswerte], [LT D14 Thr50], [WT LT D14 Thr50], [WT LT D14 Thr10], [WT LT D2 Thr10], [WT LT D6 Thr10], [WT LT D6 Thr50], [WT LT D4 Thr50 O], [WT LT D6 Thr10 O], [WT LT D6 Thr50 O], [WT LT D2 Thr50 O], [WT LT D8 Thr50 O], [WT LT D14 Thr50 O],),

    table.cell(colspan: 13)[*Panel a: globale Modelle*],
    [Basis], table.cell(fill: rgb("#e2d5ed"))[1.306], table.cell(fill: rgb("#e4d6ee"))[1.278], table.cell(fill: rgb("#e2d4ec"))[1.324], table.cell(fill: rgb("#e2d4ed"))[1.311], table.cell(fill: rgb("#e3d6ed"))[1.287], table.cell(fill: rgb("#e5d8ee"))[1.249], table.cell(fill: rgb("#e4d7ee"))[1.260], [—], [—], [—], [—], [—],
    [Imputation], table.cell(fill: rgb("#e2d4ed"))[1.314], table.cell(fill: rgb("#e3d6ed"))[1.290], table.cell(fill: rgb("#e2d4ed"))[1.318], table.cell(fill: rgb("#e2d4ec"))[1.324], table.cell(fill: rgb("#e3d5ed"))[1.303], [—], table.cell(fill: rgb("#e4d8ee"))[1.256], table.cell(fill: rgb("#e3d5ed"))[1.298], table.cell(fill: rgb("#e5d8ee"))[1.253], table.cell(fill: rgb("#e3d5ed"))[1.293], table.cell(fill: rgb("#e5d8ee"))[1.250], table.cell(fill: rgb("#e4d8ee"))[1.259],

    table.cell(colspan: 13)[*Panel b: Sektor Modelle*],
    [1. Energie (10)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#e0d2ec"))[1.354], [—], [—], [—], [—], [—],
    [2. Material: Roh- und Grundstoffe (15)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#f7f2fa"))[0.815], [—], [—], [—], [—], [—],
    [3. Industriegüter (20)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#eae0f2"))[1.115], [—], [—], [—], [—], [—],
    [4. Nicht-Basiskonsumgüter (25)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#ece2f3"))[1.076], [—], [—], [—], [—], [—],
    [5. Basiskonsumgüter (30)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#e8ddf1"))[1.162], [—], [—], [—], [—], [—],
    [6. Gesundheitswesen (35)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#eee5f4"))[1.037], [—], [—], [—], [—], [—],
    [7. Finanzen (40)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#d1bce2"))[1.721], [—], [—], [—], [—], [—],
    [8. Informationstechnologie (45)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#e4d7ee"))[1.274], [—], [—], [—], [—], [—],
    [9. Kommunikationsdienstleistungen (50)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#dccbe9"))[1.463], [—], [—], [—], [—], [—],
    [10. Versorgungsunternehmen (55)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#d0bbe1"))[1.736], [—], [—], [—], [—], [—],
    [11. Immobilien (60)], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#cdb7df"))[1.811], [—], [—], [—], [—], [—],

    table.cell(colspan: 13)[*Panel c: Stacking-Modelle*],
    [Hard Routing], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#e4d7ee"))[1.260], [—], [—], [—], [—], [—],
    [Soft Blending], [—], [—], [—], [—], [—], [—], table.cell(fill: rgb("#8e5db7"))[3.315], [—], [—], [—], [—], [—],
),