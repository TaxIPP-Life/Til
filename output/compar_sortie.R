#### comparaison entre les sorties #### 


nom = "C:/openfisca/output/"
en1 = read.csv(paste0(nom,"en1.csv"),stringsAsFactors=FALSE)
ind = read.csv(paste0(nom,"ind.csv"),stringsAsFactors=FALSE)
fam = read.csv(paste0(nom,"fam.csv"),stringsAsFactors=FALSE)
foy = read.csv(paste0(nom,"foy.csv"),stringsAsFactors=FALSE)
men = read.csv(paste0(nom,"men.csv"),stringsAsFactors=FALSE)
# 
# 
# # test h5r -> bof
# library(h5r)
# h5 = H5File("output3.h5", mode = "r")
# h5fam=getH5Group(h5, "fam")
# 
# getH5Dataset(getH5Group(h5, "fam"), "fam")
# listH5Attributes(h5fam)
# getH5Attribute(h5fam)
# h5[TITLE]
# getH5Dataset(h5, "ind")

#on tranforme les tables pour faire partir les true et false
for (ent in c("en1","ind","fam","foy","men")){
  temp = apply(get(ent), 2, as.character)
  temp[temp=="False"] = "0"
  temp[temp=="True"] =  "1"
  assign(ent, apply(temp, 2, as.numeric) )
}

for (ent in c("fam","foy","men")){
  qui = paste0("qui",ent)
  keep = which(en1[,qui]==0 )
  
}


list_diff = c()
list_diff.ent = c()
list_same = c()
list_same.ent = c()
for (nam in colnames(en1)) {
  for (ent in c("ind","fam","foy","men")){
    if (nam %in% colnames(get(ent))){
      print(paste(nam,ent))
      # on séléctionne les qui == 0 pour pouvoir comparer
      # note, on fait ça à chaque variable, ce n'est certainement pas optimal
      if (ent %in% c("fam","foy","men")){
        qui = paste0("qui",ent)
        keep = which(en1[,qui]==0 )
        nb_save = length(keep)
      }
      else {
        keep=seq(1:nrow(en1))
        nb_save = length(keep)
      }
      
      if (all(abs(en1[keep,nam] - get(ent)[1:nb_save,nam]<0.001))) {
        list_same = c(list_same,nam)
        list_same.ent = c(list_same.ent,ent)        
      }
      else {
        list_diff = c(list_diff,nam)
        list_diff.ent = c(list_diff.ent,ent)        
      }
    }
  }
}
## variables individuel
list.ind = list_diff[which(list_diff.ent=='ind')]
# checked: variable dependant de variable collective
# le problème va se résoudre de lui-même
# en1[,list.ind] - ind[,list.ind]

## variables foy
list.foy = list_diff[which(list_diff.ent=='foy')]
keep = which(en1[,"quifoy"]==0)
nb_save = length(keep)
voir1 = en1[keep,list.foy]
voir3 = foy[1:nb_save,list.foy]
voirDiff = voir3 - voir1
voirDiff = cbind(voirDiff,foy[1:nb_save,c("idfam","idmen","idfoy")] )
# list.boucl = c("cotsoc_lib", "cotsoc_bar","csgsald", "csgsali", "crdssal", "csgchoi", "csgchod","csgrstd", "csgrsti")
# en1[,list.boucl] - ind[,list.boucl]
list_zarb =c()
val = c()
for (i in 1:(nrow(voirDiff)-1)){
  if (voirDiff[i,"rng"] == -voirDiff[i+1,"rng"] & voirDiff[i,"rng"]>0){ 
    list_zarb =c(list_zarb,as.numeric(voirDiff[i,"idfoy"]),as.numeric(voirDiff[i+1,"idfoy"]))
    val = c(val,voirDiff[i,"rng"])
  } 
}
list_zarb
voir3 = cbind(voir3,foy[1:nb_save,c("idfam","idmen","idfoy")] )
voir = subset(voir3, select= (idfoy %in% list_zarb))





## variables men
list.men = list_diff[which(list_diff.ent=='men')]
keep = which(en1[,"quimen"]==0)
nb_save = length(keep)
voir1 = en1[keep,list.men]
voir3 = men[1:nb_save,list.men]
voirDiff = voir3 - voir1
voirDiff = cbind(voirDiff,men[1:nb_save,c("idfam","idmen","idfoy")] )

## variables fam
list.fam = list_diff[which(list_diff.ent=='fam')]
# to check: variable dependant de variable collective
keep = which(en1[,"quifam"]==0)
nb_save = length(keep)
voir1 = en1[keep,list.fam]
voir3 = fam[1:nb_save,list.fam]
voirDiff = voir3 - voir1
 so, loyer, coloc, zone_apl
