var indice_calor = dsData[dsIndex]['INDICE_CALOR'];
if (indice_calor == "None") {
    return "lightgrey"
}
else if (indice_calor <= 27) {
    return "cyan";
}
else if (indice_calor <= 32) {
	return "yellow";
}
else {
    return "red";
}