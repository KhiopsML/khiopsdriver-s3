#pragma once

// Taille maximale des messages construits par LoadSharedLibrary
// Il faut donc prevoir un buffer de cette taille, plus 1 pour le caractere fin de chaine
#define SHARED_LIBRARY_MESSAGE_LENGTH 512

// Chargement d'une librairie partagee.
// sLibraryPath : nom et chemin complet de la librairie (avec son extension).
// sErrorMessage : sortie qui vaut vide si le chargement a reussi, le message d'erreur sinon
// valeur de retour : handle sur la librairie ouverte, ou NULL si echec du chargement
void *LoadSharedLibrary(const char *sLibraryPath, char *sErrorMessage);

// Recuperation de l'adresse d'une fonction d'une librairie partagee.
// libraryHandle : handle valide sur une librairie deja chargee
// sfunctionName : nom de la fonction dont on veut recuperer l'adresse
// valeur de retour : adresse de la fonction, ou NULL si echec de la recherche
void *GetSharedLibraryFunction(void *libraryHandle, const char *sfunctionName);

// Liberation des ressources d'une librairie partagee deja chargee.
// libraryHandle : handle valide sur une librairie deja chargee
// valeur de retour : true si liberation effectuee, false sinon
int FreeSharedLibrary(void *libraryHandle);