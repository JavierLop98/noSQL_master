// Primera parte, datos ordenados por municipio.
// 1. Beneficiarios en el municipio de "Casalarreina"
db.PAC_municipio.aggregate(
    // Para poder hacer búsquedas por el nombre del municipio elimino de "MUNICIPIO" el código del municipio y " - "
    { $project: { "BENEFICIARIO": 1, "MUNICIPIO": 1, "Direccion": { $split: [ "$MUNICIPIO", " - " ] } , "IMPORTE_EUROS": 1} }
    { $project: { "BENEFICIARIO": 1,  "codigo_postal": { $arrayElemAt: [ "$Direccion", 0 ] }, "pueblo": { $arrayElemAt: [ "$Direccion", 1 ] } , "IMPORTE_EUROS": 1, "MEDIDA": 1} }
    {$match: {"pueblo": "Casalarreina"}}
    { $group: { _id: "$BENEFICIARIO", cobro: { $sum: "$IMPORTE_EUROS" } }}
    {$sort:{"_id": 1}})

// 1.2 Beneficiarios en el municipio de "Isar"
db.PAC_municipio.aggregate(
    { $project: { "BENEFICIARIO": 1, "MUNICIPIO": 1, "Direccion": { $split: [ "$MUNICIPIO", " - " ] } , "IMPORTE_EUROS": 1} }
    { $project: { "BENEFICIARIO": 1,  "codigo_postal": { $arrayElemAt: [ "$Direccion", 0 ] }, "pueblo": { $arrayElemAt: [ "$Direccion", 1 ] } , "IMPORTE_EUROS": 1, "MEDIDA": 1} }
    {$match: {"pueblo": "Isar"}}
    { $group: { _id: "$BENEFICIARIO", cobro: { $sum: "$IMPORTE_EUROS" } }}
    {$sort:{"_id": 1}})
    
// 1.3.1 Media beneficiarios "Isar"
db.PAC_municipio.aggregate(
    { $project: { "BENEFICIARIO": 1, "MUNICIPIO": 1, "Direccion": { $split: [ "$MUNICIPIO", " - " ] } , "IMPORTE_EUROS": 1} }
    { $project: { "BENEFICIARIO": 1,  "codigo_postal": { $arrayElemAt: [ "$Direccion", 0 ] }, "pueblo": { $arrayElemAt: [ "$Direccion", 1 ] } , "IMPORTE_EUROS": 1, "MEDIDA": 1} }
    {$match: {"pueblo": "Isar"}}
    { $group: { _id: "$BENEFICIARIO", media: { $avg: {$sum: "$IMPORTE_EUROS"} } }},
    { $group: {_id: 0, media: { $avg: "$media" } }}
    )

// 1.3.1 Media beneficiarios "Casalarreina"
db.PAC_municipio.aggregate(
    { $project: { "BENEFICIARIO": 1, "MUNICIPIO": 1, "Direccion": { $split: [ "$MUNICIPIO", " - " ] } , "IMPORTE_EUROS": 1} }
    { $project: { "BENEFICIARIO": 1,  "codigo_postal": { $arrayElemAt: [ "$Direccion", 0 ] }, "pueblo": { $arrayElemAt: [ "$Direccion", 1 ] } , "IMPORTE_EUROS": 1, "MEDIDA": 1} }
    {$match: {"pueblo": "Casalarreina"}}
    { $group: { _id: "$BENEFICIARIO", media: { $avg: {$sum: "$IMPORTE_EUROS"} } }},
    { $group: {_id: 0, media: { $avg: "$media" } }}
    )

// Segunda parte, Datos ordenados por comarcas
db.PAC_comarca.find()

// 2. Comarcas de Burgos ordenadas por la subención que reciben de mayor a menor.
db.PAC_comarca.aggregate(
    {$match:{"PROVINCIA": "Burgos"}},
    {$group:{"_id": "$COMARCA", Subvención: {$sum: "$IMPORTE_EUROS"}}}
    ).sort({"Subvención":-1})

// 2.2 Comarcas de España ordenadas por la subención que reciben de mayor a menor.
db.PAC_comarca.aggregate(
    {$group:{"_id": "$COMARCA", Subvención: {$sum: "$IMPORTE_EUROS"}}}
    ).sort({"Subvención":-1}).limit(3)

// 2.3 TOP 10 Provincia que más cobra
db.PAC_comarca.aggregate(
    {$group:{"_id": "$PROVINCIA", Subvención: {$sum: "$IMPORTE_EUROS"}}}
    ).sort({"Subvención":-1}).limit(10)

// 3. PIPELINE
// Ordenado de mayor a menor los lugares donde más cobran por sociedad o autónomo en cada comarca de Burgos.
db.PAC_comarca.aggregate(
    {$match:{"PROVINCIA": "Burgos"}},
    {$group:{"_id": ["$COMARCA","$BENEFICIARIO"], Subvencion: {$sum: "$IMPORTE_EUROS"} }},
    {$group : {"_id" :{ $arrayElemAt: ["$_id", 0]}, Subvencion: {$sum: "$Subvencion"}, N_Beneficiados:{$sum: 1}}},
    {$set: {Subvecion_por_persona: { $divide: [ "$Subvencion", "$N_Beneficiados" ] }}},
    {$sort: {"Subvecion_por_persona": -1}}
    )

// Ordenado de mayor a menor los lugares donde más cobran por sociedad o autónomo en cada comarca, SEVILLA
db.PAC_comarca.aggregate(
    {$match:{"PROVINCIA": "Sevilla"}},
    {$group:{"_id": ["$COMARCA","$BENEFICIARIO"], Subvencion: {$sum: "$IMPORTE_EUROS"} }},
    {$group : {"_id" :{ $arrayElemAt: ["$_id", 0]}, Subvencion: {$sum: "$Subvencion"}, N_Beneficiados:{$sum: 1}}},
    {$set: {Subvecion_por_persona: { $divide: [ "$Subvencion", "$N_Beneficiados" ] }}},
    {$sort: {"Subvecion_por_persona": -1}}
    )

//  Canarias
db.PAC_comarca.aggregate(
    {$match:{"PROVINCIA": "Santa Cruz de Tenerife"}},
    {$group:{"_id": ["$COMARCA","$BENEFICIARIO"], Subvencion: {$sum: "$IMPORTE_EUROS"} }},
    {$group : {"_id" :{ $arrayElemAt: ["$_id", 0]}, Subvencion: {$sum: "$Subvencion"}, N_Beneficiados:{$sum: 1}}},
    {$set: {Subvecion_por_persona: { $divide: [ "$Subvencion", "$N_Beneficiados" ] }}},
    {$sort: {"Subvecion_por_persona": -1}}
    )

// 4. TOP 10 Beneficiarios que más cobran:
db.PAC_comarca.aggregate(
    {$group:{"_id": "$BENEFICIARIO", Subvencion: {$sum: "$IMPORTE_EUROS"} }},
    {$sort: {"Subvencion": -1}}
    ).limit(10)

// Ayudas en Chantada
db.PAC_municipio.find({"MUNICIPIO": "27016 - Chantada"})

// 5. Ayudas ordenadas por tipo de medida:
db.PAC_comarca.aggregate(
    {$group:{"_id": "$MEDIDA", Subvencion: {$sum: "$IMPORTE_EUROS"} }},
    {$sort: {"Subvencion": -1}}
    ).limit(10)

// PIPELINE
db.PAC_municipio.aggregate(
    {$match: {"MUNICIPIO": "27016 - Chantada"}}
    {$group:{"_id": "$BENEFICIARIO", Total: {$sum: "$IMPORTE_EUROS"}, desglose: { $push: {Motivo: "$MEDIDA", Cantidad: "$IMPORTE_EUROS"} } }},
    { $unwind: "$desglose" }
    { $replaceWith: { $mergeObjects: [ { Beneficiario: "$_id", Total: "$Total" }, "$desglose" ] } }
    { $addFields: { "%subvencion": { "$multiply":[{"$divide":["$Cantidad","$Total"]}, 100]} } }
    {$group:{"_id": "$Motivo", Euros: {$sum: "$Cantidad"}, Subvencion: {$avg: "$%subvencion"}, Subvencionados: {$sum: 1}}}
    { $addFields: { Relacion: { "$divide":["$Euros","$Subvencionados"]}}  }
)
