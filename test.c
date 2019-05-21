#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int provenance_file_setup(char* str_in, char* file_path_out, int* level_out, char* format_out){
    //acceptable format: path=$path_str;level=$level_int;format=$format_str
    char* toklist[4] = {NULL};
    // for(int i = 0; i < 3; i++){
    //     //toklist[i] = calloc(20, sizeof(char));
    // }
    int i = 0;
    char *p;
    p = strtok(str_in, ";");
    while(p != NULL) {
        printf("%s\n", p);
        toklist[i++] = strdup(p);
        p = strtok(NULL, ";");
    }
    sscanf(toklist[0], "path=%s", file_path_out);
    sscanf(toklist[1], "level=%d", level_out);
    sscanf(toklist[2], "format=%s", format_out);

    // if(sscanf(str_in, "path=%s;level=%d;format=%s", file_path_out, level_out, format_out) == EOF)
        //return -1;
    return 0;
}

int main(int argc, char** argv){
    char* file_path_out = calloc(64, sizeof(char));
    char* format = calloc(64, sizeof(char));
    int level = 0;
    char sample_str[] = "path=prv.txt;level=2;format=";
    printf("sample_str = %s\n", sample_str);
    provenance_file_setup(sample_str, file_path_out, &level, format);
    printf("Parsing result:\npath = %s,\n level = %d,\n format =%s.\n", file_path_out, level, format);
    return 0;
}


// #include <stdio.h>
// #include <string.h>

// int main ()
// {
//   char str[] ="- This, a sample string.";
//   char * pch;
//   printf ("Splitting string \"%s\" into tokens:\n",str);
//   pch = strtok (str," ,.-");
//   while (pch != NULL)
//   {
//     printf ("%s\n",pch);
//     pch = strtok (NULL, " ,.-");
//   }
//   return 0;
// }