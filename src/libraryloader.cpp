#include "libraryloader.h"
#include <dlfcn.h>
#include <string.h>

void *LoadSharedLibrary(const char *sLibraryPath, char *sErrorMessage)
{
    void *handle;
    int i;

    // Initialisation du message d'erreur avec la chaine vide
    for (i = 0; i < SHARED_LIBRARY_MESSAGE_LENGTH + 1; i++)
    {
        sErrorMessage[i] = '\0';
    }

#if defined(__CYGWIN__)
    UINT nCurrentErrorMode;
    wchar_t *wString = NULL;
    int nBufferSize;

    // Premier appel pour determiner la taille necessaire pour stocker la chaine au format wide char
    wString = NULL;
    nBufferSize = MultiByteToWideChar(CP_ACP, 0, sLibraryPath, -1, wString, 0);

    // Second appel en ayant alloue la bonne taille
    wString = (wchar_t *)SystemObject::NewMemoryBlock(nBufferSize * sizeof(wchar_t));

    MultiByteToWideChar(CP_ACP, 0, sLibraryPath, -1, wString, nBufferSize);

    // Parametrage du mode de gestion des erreurs pour eviter d'avoir des boite de dialogues systemes bloquantes
    nCurrentErrorMode = GetErrorMode();
    SetErrorMode(SEM_FAILCRITICALERRORS);

    // Chargement de la librairie
    handle = (void *)LoadLibrary(wString);
    SystemObject::DeleteMemoryBlock(wString);

    // Restitution du mode courant de gestion des erreurs
    SetErrorMode(nCurrentErrorMode);

    // Traitement des erreures
    if (handle == NULL)
    {
        TCHAR szMessage[SHARED_LIBRARY_MESSAGE_LENGTH + 1];
        size_t nConvertedSize;
        int nErrorMessageLength;

        // Formatage du message d'erreur
        FormatMessage(FORMAT_MESSAGE_IGNORE_INSERTS |
                          FORMAT_MESSAGE_FROM_SYSTEM,
                      NULL, GetLastError(),
                      MAKELANGID(LANG_NEUTRAL, SUBLANG_NEUTRAL),
                      szMessage, MAX_PATH, NULL);

        // Conversion entre differents types de chaines de caracteres
        // https://docs.microsoft.com/fr-fr/cpp/text/how-to-convert-between-various-string-types?view=msvc-160
        wcstombs_s(&nConvertedSize, sErrorMessage, SHARED_LIBRARY_MESSAGE_LENGTH, szMessage, SHARED_LIBRARY_MESSAGE_LENGTH);

        // Supression du saut de ligne en fin de chaine
        nErrorMessageLength = (int)strlen(sErrorMessage);
        while (nErrorMessageLength > 0)
        {
            // Nettoyage si saut de ligne en fin de chaine
            if (sErrorMessage[nErrorMessageLength - 1] == '\n' or
                sErrorMessage[nErrorMessageLength - 1] == '\r')
            {
                sErrorMessage[nErrorMessageLength - 1] = '\0';
                nErrorMessageLength--;
            }
            // Ok sinon
            else
                break;
        }
    }
    return handle;

#elif defined(__unix__)
    // Nettoyage des erreurs pre-existantes
    dlerror();

    // Chargement de la bibliotheque
    handle = dlopen(sLibraryPath, RTLD_LAZY);
    if (!handle)
    {
        char *errstr;
        errstr = dlerror();
        if (errstr)
            strncpy(sErrorMessage, errstr, SHARED_LIBRARY_MESSAGE_LENGTH);
    }
    return handle;
#endif
}

void *GetSharedLibraryFunction(void *libraryHandle, const char *sFunctionName)
{
#if defined(__CYGWIN__)
    return (void *)GetProcAddress((HINSTANCE)libraryHandle, sFunctionName);
#elif defined(__unix__)
    return dlsym(libraryHandle, sFunctionName);
#endif
}

int FreeSharedLibrary(void *libraryHandle)
{
#if defined(__CYGWIN__)
    return FreeLibrary((HINSTANCE)libraryHandle);
#elif defined(__unix__)
    return dlclose(libraryHandle);
#endif
}