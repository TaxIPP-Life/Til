# On tente un match,

Tdidier <- buf[which(buf[,3]<=109),c(1,3,4)]
Tdidier[,2] =Tdidier[,2]+1900

ind$identmen <- sprintf("%06d",match(ind$identmen,men$identmen))
ind$identind <- paste(ind$identmen,ind$noi)
Tind    <- subset(ind, selec=c(identind,identmen,anais,sexe))



limit      <- max( nrow(Tdidier), nrow(Tind))
taille.m   <- 15006

i=1
j=1
identmen = numeric( nrow(Tdidier))
while (j<(100+1) & i<(nrow(Tind)+1)) {
  print( paste(j,i))
  taille.men = length( which(Tind$identmen[] == Tind$identmen[i])  )
  print(taille.men)
  if (Tdidier[j,2] == Tind$anais[i]) {
    if (identical(Tdidier[j:(j+taille.men-1),2] ,Tind$anais[i:(i+taille.men-1)])) {
       identmen[j:(j+taille.men-1)] = Tind$identmen[i:(i+taille.men-1)]
       i = i+taille.men
       j= j+taille.men
       print("marche")
    }
    else {
      j=j+1
      print("pas loin")
      }
  }
  else { 
    i=j+1
    print("marche pas")
    }
}



