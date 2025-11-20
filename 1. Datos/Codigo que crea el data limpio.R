# Paquetes
paquetes <- c("readr","readxl","dplyr","purrr","stringr","janitor","lubridate")
invisible(lapply(paquetes, function(p) if(!requireNamespace(p, quietly = TRUE)) install.packages(p)))
library(readr); library(readxl); library(dplyr); library(purrr); library(stringr); library(janitor); library(lubridate)

# Rutas (ya conocidas)
dir_proyecto <- "C://Users//Jonat//Documents//Diplomado//tarea_mod_8"
dir_conjunto <- file.path(dir_proyecto, "conjunto_de_datos")
dir_dic     <- file.path(dir_proyecto, "diccionario_de_datos")
dir_catalogos <- file.path(dir_proyecto, "catalogos")
dir_salida  <- file.path(dir_proyecto, "output")
dir.create(dir_salida, showWarnings = FALSE)

# 1) localizar archivos anuales y diccionario
archivos_anuales <- list.files(dir_conjunto, pattern = "atus_anual_\\d{4}\\.csv$", full.names = TRUE)
if(length(archivos_anuales) == 0) stop("No encontré archivos anuales en 'conjunto_de_datos'.")
ruta_diccionario <- list.files(dir_dic, pattern = "\\.csv$", full.names = TRUE)[1]
if(is.na(ruta_diccionario)) stop("No encontré diccionario en 'diccionario_de_datos'.")

# 2) leer diccionario forzando carácter y detectar columnas relevantes
diccionario <- readr::read_csv(ruta_diccionario, col_types = cols(.default = "c"), show_col_types = FALSE)

# detectar heurísticamente la columna con el nombre de la variable y la columna con el tipo
col_nombre_dic <- names(diccionario)[which(str_detect(names(diccionario), regex("original|campo|variable|nombre|field|name", ignore_case = TRUE)) )][1]
col_tipo_dic   <- names(diccionario)[which(str_detect(names(diccionario), regex("tipo|type|clasif|class|data", ignore_case = TRUE)) )][1]

# si no se detectan, usar las dos primeras columnas
if(is.na(col_nombre_dic)) col_nombre_dic <- names(diccionario)[1]
if(is.na(col_tipo_dic)) col_tipo_dic <- names(diccionario)[2]

mapa_tipos_raw <- diccionario %>%
  select(!!col_nombre_dic, !!col_tipo_dic) %>%
  rename(original = !!col_nombre_dic, tipo = !!col_tipo_dic) %>%
  mutate(original = as.character(original), tipo = as.character(tipo)) %>%
  filter(!is.na(original) & original != "")

# 3) normalizar nombres para hacer matching con los csv (usar clean names)
mapa_tipos <- mapa_tipos_raw %>%
  mutate(nombre_limpio = janitor::make_clean_names(original),
         tipo_norm = str_to_upper(tipo) %>% str_squish())

# función que mapea texto del diccionario a un tipo R
mapear_tipo_r <- function(texto_tipo){
  txt <- ifelse(is.na(texto_tipo), "", str_to_lower(texto_tipo))
  if(str_detect(txt, "int|enter|entero|integer")) return("integer")
  if(str_detect(txt, "num|decim|double|float|numeric|real")) return("numeric")
  if(str_detect(txt, "fecha|date|datetime|time")) return("date")
  if(str_detect(txt, "log|bool|boolean|si/no|sí/no")) return("logical")
  # por defecto: carácter
  return("character")
}

mapa_tipos <- mapa_tipos %>% mutate(tipo_r = map_chr(tipo_norm, mapear_tipo_r))

# 4) leer todos los CSV anuales como carácter (evita conflictos de tipos)
leer_anual_como_caracter <- function(ruta){
  readr::read_csv(ruta, col_types = cols(.default = "c"), locale = locale(encoding = "UTF-8"), show_col_types = FALSE)
}

lista_dfs <- map(archivos_anuales, function(ruta){
  message("Leyendo: ", basename(ruta))
  df <- leer_anual_como_caracter(ruta)
  # normalizar nombres de columnas para matching
  names(df) <- janitor::make_clean_names(names(df))
  # conservar información de origen y año detectado
  anio_archivo <- str_extract(basename(ruta), "\\d{4}")
  df <- df %>% mutate(archivo_origen = basename(ruta), anio_archivo = anio_archivo)
  return(df)
})
names(lista_dfs) <- basename(archivos_anuales)

# 5) identificar columnas objetivo en el conjunto unido (unión de nombres)
nombres_unicos <- lista_dfs %>% map(names) %>% reduce(union)

# 6) para cada df, asegurarse de que tenga todas las columnas (llenar NA si falta)
lista_dfs_uniformes <- map(lista_dfs, function(df){
  faltantes <- setdiff(nombres_unicos, names(df))
  if(length(faltantes)>0){
    df[faltantes] <- NA_character_
  }
  # reordenar columnas para consistencia
  df <- df %>% select(all_of(nombres_unicos))
  df
})

# 7) convertir columnas según mapa_tipos cuando existan
# construir lista de conversiones: nombres en limpio -> tipo_r
conversiones <- mapa_tipos %>% select(nombre_limpio, tipo_r) %>% distinct() %>% filter(nombre_limpio %in% nombres_unicos)

convertir_segmento <- function(df){
  for(i in seq_len(nrow(conversiones))){
    col <- conversiones$nombre_limpio[i]
    tipo <- conversiones$tipo_r[i]
    if(!(col %in% names(df))) next
    if(tipo == "numeric"){
      # reemplazar comas decimales por punto y convertir
      df[[col]] <- df[[col]] %>% 
        na_if("") %>%
        str_replace_all("\\,", ".") %>%
        as.numeric()
    } else if(tipo == "integer"){
      df[[col]] <- df[[col]] %>% na_if("") %>% str_replace_all("\\,", ".") %>% as.numeric() %>% as.integer()
    } else if(tipo == "date"){
      # intentar parsear con varios formatos
      df[[col]] <- parse_date_time(df[[col]], orders = c("Ymd", "dmy", "mdy", "Y-m-d", "d/m/Y", "m/d/Y"), truncated = 3)
      df[[col]] <- as.Date(df[[col]])
    } else if(tipo == "logical"){
      df[[col]] <- case_when(
        tolower(df[[col]]) %in% c("si","sí","s","1","true","t") ~ TRUE,
        tolower(df[[col]]) %in% c("no","n","0","false","f") ~ FALSE,
        TRUE ~ NA
      )
    } else {
      # character: dejar como está y limpiar espacios
      df[[col]] <- df[[col]] %>% na_if("") %>% str_squish()
    }
  }
  return(df)
}

lista_dfs_convertidas <- map(lista_dfs_uniformes, convertir_segmento)

# 8) unir todos los años en un solo data.frame (ahora con tipos consistentes para las columnas mapeadas)
datos_unidos <- bind_rows(lista_dfs_convertidas)

# 9) manejar columnas que quedaron con sufijos por duplicidad (si existen)
# (si hubo nombres idénticos con ...2, ...3 tras lecturas, unificarlos)
# usar clean_names para uniformidad ya se hizo antes, este paso es preventivo
datos_unidos <- datos_unidos %>% janitor::clean_names()

# 10) guardar resultado unido y por año
ruta_unido <- file.path(dir_salida, "datos_anuales_unidos.csv")
write_csv(datos_unidos, ruta_unido)
message("Guardado: ", ruta_unido)

# guardar por año detectado (campo anio_archivo)
if("anio_archivo" %in% names(datos_unidos) && any(!is.na(datos_unidos$anio_archivo))){
  anos <- sort(unique(na.omit(datos_unidos$anio_archivo)))
  for(a in anos){
    ruta_a <- file.path(dir_salida, paste0("datos_anuales_unidos_", a, ".csv"))
    write_csv(filter(datos_unidos, anio_archivo == a), ruta_a)
  }
  message("Archivos por año escritos en: ", dir_salida)
} else {
  message("No se detectó columna 'anio_archivo' o está vacía.")
}

# 11) leer y guardar catálogos (tc_*.csv) por si se requieren para joins posteriores
catalogos <- list()
if(dir.exists(dir_catalogos)){
  archivos_catalogos <- list.files(dir_catalogos, pattern = "\\.csv$", full.names = TRUE)
  for(r in archivos_catalogos){
    nombre <- basename(r) %>% str_remove("\\.csv$")
    catalogos[[nombre]] <- readr::read_csv(r, col_types = cols(.default = "c"), show_col_types = FALSE)
  }
  saveRDS(catalogos, file.path(dir_salida, "catalogos_lista.rds"))
  message("Catálogos guardados en output (RDS).")
}

# Resumen final
message("Registros finales: ", nrow(datos_unidos))
message("Columnas finales (muestra): ", paste(head(names(datos_unidos), 40), collapse = ", "))


#################
paquetes <- c("data.table","readr","janitor","stringr","lubridate")
invisible(lapply(paquetes, function(p) if(!requireNamespace(p, quietly = TRUE)) install.packages(p)))
library(data.table); library(readr); library(janitor); library(stringr); library(lubridate)

# Rutas (ajustar solo si no están definidas en la sesión)
if(!exists("dir_proyecto")) dir_proyecto <- "C://Users//Jonat//Documents//Diplomado//tarea_mod_8"
dir_conjunto   <- file.path(dir_proyecto, "conjunto_de_datos")
dir_dic        <- file.path(dir_proyecto, "diccionario_de_datos")
dir_catalogos  <- file.path(dir_proyecto, "catalogos")
dir_salida     <- if(exists("dir_salida") && nzchar(dir_salida)) dir_salida else file.path(dir_proyecto, "output")
dir.create(dir_salida, showWarnings = FALSE, recursive = TRUE)

# 0) Cargar o generar datos_unidos
if(!exists("datos_unidos")){
  archivos_anuales <- list.files(dir_conjunto, pattern = "atus_anual_\\d{4}\\.csv$", full.names = TRUE)
  if(length(archivos_anuales) == 0) stop("No encontré archivos anuales en 'conjunto_de_datos' y no existe 'datos_unidos'.")
  # leer en data.table de forma eficiente (leer como character por columnas variables)
  lista <- lapply(archivos_anuales, function(f){
    message("Leyendo: ", basename(f))
    DTf <- fread(f, encoding = "UTF-8", colClasses = "character", showProgress = FALSE)
    setnames(DTf, janitor::make_clean_names(names(DTf)))
    DTf[, archivo_origen := basename(f)]
    DTf[, anio_archivo := str_extract(archivo_origen, "\\d{4}")]
    DTf
  })
  datos_unidos <- rbindlist(lista, use.names = TRUE, fill = TRUE)
  rm(lista); gc()
}

# 1) convertir a data.table por referencia
DT <- if(inherits(datos_unidos, "data.table")) copy(datos_unidos) else as.data.table(datos_unidos)

# 2) leer catálogos (tc_*.csv) si existen
catalogos <- list()
if(dir.exists(dir_catalogos)){
  archivos_cat <- list.files(dir_catalogos, pattern = "\\.csv$", full.names = TRUE)
  for(f in archivos_cat){
    nm <- tolower(gsub("\\.csv$","",basename(f)))
    try({
      ct <- fread(f, encoding = "UTF-8", colClasses = "character", showProgress = FALSE)
      setnames(ct, janitor::make_clean_names(names(ct)))
      catalogos[[nm]] <- ct
    }, silent = TRUE)
  }
}

# helper: crear vector mapeo clave->valor desde catálogo (primeras 2 columnas por defecto)
crear_map <- function(cat_dt){
  if(is.null(cat_dt)) return(NULL)
  if(ncol(cat_dt) < 2) return(NULL)
  claves <- as.character(cat_dt[[1]])
  valores <- as.character(cat_dt[[2]])
  idx <- !is.na(claves) & claves != ""
  if(!any(idx)) return(NULL)
  m <- valores[idx]; names(m) <- claves[idx]
  m[!duplicated(names(m))]
}
map_hora     <- crear_map(catalogos[["tc_hora"]])
map_minuto   <- crear_map(catalogos[["tc_minuto"]])
map_entidad  <- crear_map(catalogos[["tc_entidad"]])
# no usar crear_map solo para municipio porque se requiere matching más robusto; leer catálogo completo si existe
cat_municipio <- if(!is.null(catalogos[["tc_municipio"]])) copy(catalogos[["tc_municipio"]]) else NULL

# 3) normalizar identificadores en DT
DT[, id_entidad_chr := if("id_entidad" %in% names(DT)) as.character(id_entidad) else NA_character_]
DT[, id_municipio_chr := if("id_municipio" %in% names(DT)) as.character(id_municipio) else NA_character_]
DT[, id_hora_chr := if("id_hora" %in% names(DT)) as.character(id_hora) else NA_character_]
DT[, id_minuto_chr := if("id_minuto" %in% names(DT)) as.character(id_minuto) else NA_character_]

# 4) componentes fecha y hora (crea NA cuando falten o fuera de rango)
DT[, anio_t := if("anio" %in% names(DT)) suppressWarnings(as.integer(as.character(anio))) else NA_integer_]
DT[, mes_t  := if("mes"  %in% names(DT)) suppressWarnings(as.integer(as.character(mes)))  else NA_integer_]
DT[, dia_t  := if("id_dia" %in% names(DT)) suppressWarnings(as.integer(as.character(id_dia))) else NA_integer_]
DT[, hora_t := suppressWarnings(as.integer(id_hora_chr))]
DT[, minuto_t := suppressWarnings(as.integer(id_minuto_chr))]

DT[mes_t < 1 | mes_t > 12, mes_t := NA_integer_]
DT[dia_t < 1 | dia_t > 31, dia_t := NA_integer_]
DT[hora_t < 0 | hora_t > 23, hora_t := NA_integer_]
DT[minuto_t < 0 | minuto_t > 59, minuto_t := NA_integer_]

# construir fecha_del_accidente y fecha_hora_accidente (UTC)
DT[, fecha_del_accidente := as.IDate(NA)]
idx_f <- which(!is.na(DT$anio_t) & !is.na(DT$mes_t) & !is.na(DT$dia_t))
if(length(idx_f) > 0) DT[idx_f, fecha_del_accidente := as.IDate(sprintf("%04d-%02d-%02d", anio_t, mes_t, dia_t))]

DT[, fecha_hora_accidente := as.POSIXct(NA)]
idx_dt <- which(!is.na(DT$fecha_del_accidente) & !is.na(DT$hora_t))
if(length(idx_dt) > 0) {
  DT[idx_dt, fecha_hora_accidente := as.POSIXct(sprintf("%s %02d:%02d:00",
                                                        as.character(fecha_del_accidente[idx_dt]),
                                                        hora_t[idx_dt],
                                                        ifelse(is.na(minuto_t[idx_dt]), 0L, minuto_t[idx_dt])),
                                                format = "%Y-%m-%d %H:%M:%S", tz = "UTC")]
}

# 5) mapear entidad simple (clave->nombre) si catálogo disponible, sino usar id_entidad
if(!is.null(map_entidad) & "id_entidad" %in% names(DT)){
  DT[, entidad := map_entidad[as.character(id_entidad)] ]
  DT[is.na(entidad), entidad := as.character(id_entidad)]
} else if("id_entidad" %in% names(DT)){
  DT[, entidad := as.character(id_entidad)]
} else if("entidad" %in% names(DT)){
  DT[, entidad := as.character(entidad)]
} else {
  DT[, entidad := NA_character_]
}

# 7) tipo_de_vehiculo (indicadores binarios -> etiqueta; fallback textual)
veh_prior <- c("automovil","camioneta","camion","motociclet","motocicleta","omnibus","microbus","pascamion","bicicleta","tractor","tranvia","ferrocarri","otrovehic","otro_vehic")
veh_exist <- intersect(names(DT), veh_prior)
if(length(veh_exist) > 0){
  for(cn in veh_exist) set(DT, j = cn, value = suppressWarnings(as.integer(as.character(DT[[cn]]))))
  DT[, tipo_de_vehiculo := NA_character_]
  for(cn in veh_prior[veh_prior %in% names(DT)]){
    etiqueta <- switch(cn,
                       automovil="Automóvil", camioneta="Camioneta", camion="Camión",
                       motociclet="Motocicleta", motocicleta="Motocicleta",
                       omnibus="Ómnibus", microbus="Microbús", pascamion="Vehículo de carga",
                       bicicleta="Bicicleta", tractor="Tractor", tranvia="Tranvía", ferrocarri="Ferrocaril",
                       otrovehic="Otro", otro_vehic="Otro", "Otro")
    DT[is.na(tipo_de_vehiculo) & !is.na(get(cn)) & get(cn) > 0, tipo_de_vehiculo := etiqueta]
  }
  txt_fallback <- intersect(names(DT), c("tipo_vehiculo","vehiculo","vehicul","vehiculo_tipo"))
  if(length(txt_fallback) > 0) DT[is.na(tipo_de_vehiculo), tipo_de_vehiculo := as.character(get(txt_fallback[1]))]
} else {
  if("tipo_vehiculo" %in% names(DT)) DT[, tipo_de_vehiculo := as.character(tipo_vehiculo)]
  else if("vehiculo" %in% names(DT)) DT[, tipo_de_vehiculo := as.character(vehiculo)]
  else DT[, tipo_de_vehiculo := NA_character_]
}

# 8) tipo_de_accidente
if("tipaccid" %in% names(DT)) DT[, tipo_de_accidente := as.character(tipaccid)] else if("tipo_accidente" %in% names(DT)) DT[, tipo_de_accidente := as.character(tipo_accidente)] else DT[, tipo_de_accidente := NA_character_]

# 9) estado_de_la_victima (Fallecido / Lesionado / Ileso) agrupando indicadores
cols_m <- intersect(names(DT), c("condmuerto","pasamuerto","peatmuerto","ciclmuerto","otromuerto","nemuerto"))
cols_h <- intersect(names(DT), c("condherido","pasaherido","peatherido","ciclherido","otroherido","neherido"))
if(length(cols_m) > 0) for(cn in cols_m) set(DT, j = cn, value = suppressWarnings(as.integer(as.character(DT[[cn]]))))
if(length(cols_h) > 0) for(cn in cols_h) set(DT, j = cn, value = suppressWarnings(as.integer(as.character(DT[[cn]]))))
DT[, muertos_total := if(length(cols_m) > 0) rowSums(.SD, na.rm = TRUE) else 0L, .SDcols = cols_m]
DT[, heridos_total := if(length(cols_h) > 0) rowSums(.SD, na.rm = TRUE) else 0L, .SDcols = cols_h]
DT[, estado_de_la_victima := fifelse(muertos_total > 0, "Fallecido", fifelse(heridos_total > 0, "Lesionado", "Ileso"))]
DT[, c("muertos_total","heridos_total") := NULL]

# 10) genero, edad_aproximada
DT[, genero := if("sexo" %in% names(DT)) as.character(sexo) else NA_character_]
if("id_edad" %in% names(DT)) {
  DT[, edad_aproximada := suppressWarnings(as.integer(as.character(id_edad)))]
} else if("edad_num" %in% names(DT)) {
  DT[, edad_aproximada := suppressWarnings(as.integer(as.character(edad_num)))]
} else {
  DT[, edad_aproximada := NA_integer_]
}

# 11) hora_minuto, periodo_hora
#11) hora_minuto y periodo_hora (bloque corregido y autónomo)
# Pegar en la sesión donde ya existen: DT, map_hora, map_minuto, id_hora_chr, id_minuto_chr, hora_t, minuto_t

# asegurar columnas base
if(!"hora_del_dia" %in% names(DT)) DT[, hora_del_dia := NA_integer_]
if(!"minuto" %in% names(DT))      DT[, minuto := NA_integer_]
if(!"hora_catalogo" %in% names(DT)) DT[, hora_catalogo := NA_character_]
if(!"minuto_catalogo" %in% names(DT)) DT[, minuto_catalogo := NA_character_]

# 1) completar hora_del_dia desde fuentes posibles (prioridad: hora_t -> map_hora)
if("hora_t" %in% names(DT)) {
  DT[is.na(hora_del_dia) & !is.na(hora_t), hora_del_dia := as.integer(hora_t)]
}
if(!is.null(map_hora) && "id_hora_chr" %in% names(DT)) {
  # map_hora puede devolver texto; intentar coerción segura
  DT[is.na(hora_del_dia) & !is.na(id_hora_chr), hora_catalogo := map_hora[id_hora_chr]]
  DT[is.na(hora_del_dia) & !is.na(hora_catalogo), hora_del_dia := suppressWarnings(as.integer(hora_catalogo))]
}

# 2) completar minuto desde fuentes posibles (prioridad: minuto_t -> map_minuto)
if("minuto_t" %in% names(DT)) {
  DT[is.na(minuto) & !is.na(minuto_t), minuto := as.integer(minuto_t)]
}
if(!is.null(map_minuto) && "id_minuto_chr" %in% names(DT)) {
  DT[is.na(minuto) & !is.na(id_minuto_chr), minuto_catalogo := map_minuto[id_minuto_chr]]
  DT[is.na(minuto) & !is.na(minuto_catalogo), minuto := suppressWarnings(as.integer(minuto_catalogo))]
}

# 3) validación de rangos y normalización
DT[hora_del_dia < 0 | hora_del_dia > 23, hora_del_dia := NA_integer_]
DT[minuto < 0 | minuto > 59, minuto := NA_integer_]

# 4) construir hora_minuto preferente
DT[, hora_minuto := NA_character_]
# si tenemos hora numérica, usarla (minuto a 00 si NA)
DT[!is.na(hora_del_dia), hora_minuto := sprintf("%02d:%02d", hora_del_dia, ifelse(is.na(minuto), 0L, minuto))]
# si aún NA y existen etiquetas de catálogo, combinarlas
DT[is.na(hora_minuto) & !is.na(hora_catalogo) & !is.na(minuto_catalogo), hora_minuto := paste0(hora_catalogo, ":", minuto_catalogo)]
DT[is.na(hora_minuto) & !is.na(hora_catalogo) & is.na(minuto_catalogo), hora_minuto := hora_catalogo]

# 5) periodo del día
DT[, periodo_hora := NA_character_]
DT[!is.na(hora_del_dia) & hora_del_dia <= 5,  periodo_hora := "Madrugada"]
DT[!is.na(hora_del_dia) & hora_del_dia >= 6 & hora_del_dia <= 11, periodo_hora := "Mañana"]
DT[!is.na(hora_del_dia) & hora_del_dia >= 12 & hora_del_dia <= 17, periodo_hora := "Tarde"]
DT[!is.na(hora_del_dia) & hora_del_dia >= 18 & hora_del_dia <= 23, periodo_hora := "Noche"]

# 6) garantía: columnas mínimas creadas
cols_check <- c("hora_del_dia","minuto","hora_catalogo","minuto_catalogo","hora_minuto","periodo_hora")
for(cn in cols_check) if(!(cn %in% names(DT))) DT[, (cn) := NA_character_]


# 12) causa probable del accidente y variables auxiliares solicitadas
if("causaacci" %in% names(DT)) DT[, causa_probable_del_accidente := as.character(causaacci)] else if("causa" %in% names(DT)) DT[, causa_probable_del_accidente := as.character(causa)] else DT[, causa_probable_del_accidente := NA_character_]

DT[, aliento := if("aliento" %in% names(DT)) as.character(aliento) else NA_character_]
DT[, cinturon := if("cinturon" %in% names(DT)) as.character(cinturon) else NA_character_]
DT[, dia_de_la_semana := if("diasemana" %in% names(DT)) as.character(diasemana) else NA_character_]
DT[, cobertura := if("cobertura" %in% names(DT)) as.character(cobertura) else NA_character_]
DT[, urbana := if("urbana" %in% names(DT)) as.character(urbana) else NA_character_]
DT[, suburbana := if("suburbana" %in% names(DT)) as.character(suburbana) else NA_character_]
DT[, caparod := if("caparod" %in% names(DT)) as.character(caparod) else NA_character_]
DT[, tipaccid := if("tipaccid" %in% names(DT)) as.character(tipaccid) else NA_character_]
DT[, clasacc := if("clasacc" %in% names(DT)) as.character(clasacc) else NA_character_]
DT[, estatus := if("estatus" %in% names(DT)) as.character(estatus) else NA_character_]
DT[, archivo_origen := if("archivo_origen" %in% names(DT)) as.character(archivo_origen) else NA_character_]
DT[, anio_archivo := if("anio_archivo" %in% names(DT)) as.character(anio_archivo) else as.character(anio)]

# 13) columnas finales (en español). Garantizar existencia y orden.
cols_finales <- c(
  "fecha_del_accidente","fecha_hora_accidente",
  "tipo_de_vehiculo","tipo_de_accidente","estado_de_la_victima","genero","edad_aproximada",
  "causa_probable_del_accidente",
  "aliento","cinturon","dia_de_la_semana",
  "entidad","municipio",
  "caparod","tipaccid","clasacc","estatus","urbana"
)
for(col in cols_finales) if(!(col %in% names(DT))) DT[, (col) := NA_character_]

DT_min_ext <- DT[, ..cols_finales]


setDT(DT_min_ext)



# 14) guardar comprimido y asignar objetos globales
outfile <- file.path(dir_salida, "datos_minimos_accidentes_extendido.csv.gz")
fwrite(DT_min_ext, outfile, compress = "gzip")
assign("datos_minimos_accidentes_extendido", DT_min_ext, envir = .GlobalEnv)
assign("datos_minimos_accidentes", DT_min_ext, envir = .GlobalEnv)

#########################

DT <- data.table::as.data.table(DT_min_ext)

# 1) Normalizaciones básicas (asegurar tipos y valores razonables)
DT[, fecha_del_accidente := as.IDate(fecha_del_accidente)]                 # ya puede ser NA
DT[, tipo_de_vehiculo := as.character(tipo_de_vehiculo)]
DT[, tipo_de_accidente := as.character(tipo_de_accidente)]
DT[, estado_de_la_victima := as.character(estado_de_la_victima)]
DT[, genero := as.character(genero)]
DT[, edad_aproximada := suppressWarnings(as.integer(edad_aproximada))]
DT[, causa_probable_del_accidente := as.character(causa_probable_del_accidente)]
DT[, entidad := as.character(entidad)]
DT[, municipio := as.character(municipio)]
DT[, aliento := as.character(aliento)]
DT[, cinturon := as.character(cinturon)]
DT[, dia_de_la_semana := as.character(dia_de_la_semana)]

# 2) Correcciones puntuales
# edad fuera de rango -> NA
DT[!is.na(edad_aproximada) & (edad_aproximada < 0 | edad_aproximada > 110), edad_aproximada := NA_integer_]

# estandarizar genero
DT[, genero := trimws(tolower(genero))]
DT[genero %in% c("m","hombre","masculino","varon","varón","male"), genero := "Hombre"]
DT[genero %in% c("f","mujer","femenino","female"), genero := "Mujer"]
DT[!genero %in% c("Hombre","Mujer"), genero := NA_character_]

# estandarizar estado_de_la_victima
DT[, estado_de_la_victima := trimws(tolower(estado_de_la_victima))]
DT[estado_de_la_victima %in% c("fallecido","muerto","fallecida","muerte","dead"), estado_de_la_victima := "Fallecido"]
DT[estado_de_la_victima %in% c("herido","lesionado","lesionada","lesionado(a)"), estado_de_la_victima := "Lesionado"]
DT[estado_de_la_victima %in% c("ileso","sin lesion","sin lesiones","solo daños","solo daños","solo daños"), estado_de_la_victima := "Ileso"]
DT[!estado_de_la_victima %in% c("Fallecido","Lesionado","Ileso"), estado_de_la_victima := NA_character_]



cols_esenciales <- c( "fecha_del_accidente", "hora_minuto", "tipo_de_vehiculo", "tipo_de_accidente", "estado_de_la_victima", "genero", "edad_aproximada", "causa_probable_del_accidente", "entidad", "municipio" )
# 3) Columnas esenciales que queremos conservar válidas (ajustar si hace falta)
# columnas esenciales que realmente existen en DT
cols_esenciales_existentes <- intersect(cols_esenciales, names(DT))

# no exigir 'municipio'
cols_requeridas <- setdiff(cols_esenciales_existentes, "municipio")

# si hay columnas requeridas, eliminar filas que tengan NA en cualquiera de ellas
if(length(cols_requeridas) > 0){
  # cálculo vectorizado y eficiente de NAs por fila en las columnas requeridas
  nas_por_fila <- rowSums(is.na(DT[, ..cols_requeridas]))
  DT <- DT[nas_por_fila == 0]
}

# reasignar resultado limpio y guardar (mantener comportamiento previo)
datos_minimos_accidentes_limpio <- DT
assign("datos_minimos_accidentes_limpio", datos_minimos_accidentes_limpio, envir = .GlobalEnv)

ruta_salida <- file.path(if(exists("dir_salida") && nzchar(dir_salida)) dir_salida else getwd(),
                         "datos_minimos_accidentes_limpio.csv.gz")
data.table::fwrite(datos_minimos_accidentes_limpio, ruta_salida, compress = "gzip")
ruta_rdata <- file.path(dir_salida, "datos_minimos_accidentes_limpio.RData")
save(datos_minimos_accidentes_limpio, file = ruta_rdata, compress = "xz") 


message("Registros finales (sin requerir 'municipio'): ", nrow(datos_minimos_accidentes_limpio))
message("Guardado: ", ruta_salida)

