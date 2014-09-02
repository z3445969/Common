/*
 * src/tutorial/complex.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */
#include "access/hash.h"
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>



PG_MODULE_MAGIC;

void checkString(char *str);

//Switch to the new struct
typedef struct Email
{
	/* The structure will contain both Local and Domain part of 1 Email Address
	 * Local@Domain#
	 * Follow the statement there is no sign except . and - in the string
	 * So we can use @ and # to compute the length of local and domain part
	 * The maximum string will be 129 + 129 + 1(for @) + 1(for #);
	 */
	char data[260];
}	Email;

/*
 * Since we use V1 function calling convention, all these functions have
 * the same signature as far as C is concerned.  We provide these prototypes
 * just to forestall warnings when compiled with gcc -Wmissing-prototypes.
 */
Datum		email_in(PG_FUNCTION_ARGS);
Datum		email_out(PG_FUNCTION_ARGS);
Datum		email_recv(PG_FUNCTION_ARGS);
Datum		email_send(PG_FUNCTION_ARGS);
Datum		email_eq(PG_FUNCTION_ARGS);
Datum		email_gt(PG_FUNCTION_ARGS);
Datum		email_domain_eq(PG_FUNCTION_ARGS);
Datum		email_not_eq(PG_FUNCTION_ARGS);
Datum		email_gt_eq(PG_FUNCTION_ARGS);
Datum		email_lt(PG_FUNCTION_ARGS);
Datum		email_lt_eq(PG_FUNCTION_ARGS);
Datum		email_not_domain_eq(PG_FUNCTION_ARGS);
Datum		email_cmp(PG_FUNCTION_ARGS);
Datum		email_hash(PG_FUNCTION_ARGS);

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_in);

Datum
email_in(PG_FUNCTION_ARGS)
{
	char   *local;
	char   *domain;
	char *in = (char *) PG_GETARG_POINTER(0);
	int count;
	int isDomain;
	char *str;	
	char *delimiter;
	Email    *result; 
		
	local = (char *) palloc0(129);
    	domain = (char *) palloc0(129);
	*domain = '\0';
	*local = '\0';
	isDomain = 0;
	str = local;
	count = 0;

	while (*in != '\0') {
		if (*in == '@') {
			if (isDomain == 0) {
				isDomain = 1;
				count = 0;
				*str = '\0';
				str = domain;
			}
			else {
				ereport(ERROR, 
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("Error: Cannot have more than one '@' in an email")));
			}
		}
		else {
			if (count == 128) {
				ereport(ERROR, 
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("Error: Only 128 characters allowed in Local or Domain part")));
			}
			*str++ = tolower(*in);
			count++;
		}
		in++;
	}

	*str = '\0';
	str = local;
    	checkString(str);
	str = domain;
	//In the case of Domain part we have to add this check before the main check above
	//Special check if there is a . exist in the string  
	delimiter = strchr(domain,'.');
   	if(delimiter==NULL){
		ereport(ERROR, 
		(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		 errmsg("Error: Domain part must contain at least one '.'")));  
    	}
	checkString(str);
	result = (Email *) palloc0(sizeof(Email)); 
	//Now we copy the content of email address to our data
	//Remember the length of the local


	//Adding the region to output result
	memmove(result->data,local,strlen(local));
	str = result->data;
	str = str+strlen(local);
	*str = '@';
	str++;	
	memmove(str , domain , strlen(domain) );
	str = str+strlen(domain);
	*str = '#';
	str++;
	*str = '\0';
	//Now return the result and free the alloc before that
	pfree(local);
	pfree(domain);
	
	PG_RETURN_POINTER(result);
}

//Function to check the content of Local and Domain part of EmailAddress
void checkString(char *str) {

	//Check the first character of the part is a letter
    	if(!isalpha(*str)){
        	ereport(ERROR, 
		(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		errmsg("Error: Only a letter can begin a word")));
   	 }

    	//Loop through the part
    	//Check if there is another sign except . amd - in the string
    	//Check the format of the rest
    	while(*str != '\0'){
        	if(!isalnum(*str)){
        
            		if(*str != '.' && *str != '-'){
				ereport(ERROR, 
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		 		errmsg("Error: Only letters, numbers, '.', and '-' allowed")));
            		}
            		//Check if after a . there is only letter
            		if(*str=='.'){
                		//A word begin with a letter
                		if(!isalpha(*(str+1))){
					ereport(ERROR, 
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		 			errmsg("Error: Only a letter can begin a word")));
                		}
                		//A word end with a letter or digit
                		if(!isalnum(*(str-1))){
					ereport(ERROR, 
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		 			errmsg("Error: Only a letter or digit can end a word")));
                		}
			}
            	}
		str++;
        }
    
    	//Last check if the end is a number or a digit
    	if(!isalnum(*(str-1))){
        	ereport(ERROR, 
		(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		errmsg("Error: Only a letter or dtempigit can end a word")));
    	}

}

PG_FUNCTION_INFO_V1(email_out);
Datum email_out(PG_FUNCTION_ARGS)
{
	//Format of the email contain an # to signal the end of string 
	//When output we need to getrid of this ending	
	Email    *email = (Email *) PG_GETARG_POINTER(0);
	char *result;
	char *temp;
	char *tempRun;		
	
	result = (char *) palloc0(260);
	tempRun = result;
	temp = email->data;
	while(*temp!='#'){
		*tempRun = *temp;
		temp++;
		tempRun++;	
	}
	*tempRun='\0';	
	PG_RETURN_CSTRING(result);
}

/*****************************************************************************
 * Binary Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(complex_recv);
Datum email_recv(PG_FUNCTION_ARGS)
{

	char *local;
	char *domain;
	Email *input = (Email *) PG_GETARG_POINTER(0);
	char *in;
	int count;
	int isDomain;
	char *str;	
	Email    *result; 
	
	//Prepare the variable and testing input
	in = input->data;
	local = (char *) palloc0(129);
    	domain = (char *) palloc0(129);
	*domain = '\0';
	*local = '\0';
	isDomain = 0;
	str = local;
	count = 0;

	while (*in != '#') {
		if (*in == '@') {
			if (isDomain == 0) {
				isDomain = 1;
				count = 0;
				*str = '\0';
				str = domain;
			}
		}
		else {
			
			*str++ = tolower(*in);
			count++;
		}
		in++;
	}

	*str = '\0';

	//Merge both local and domain to 1 string 
	result = (Email *) palloc0(sizeof(Email));
	//Now we copy the content of email address to our data
	//Remember the length of the local
	//Adding the region to output result
	memmove(result->data,local,strlen(local));
	str = result->data;
	str = str+strlen(local);
	*str = '@';
	str++;	
	memmove(str , domain , strlen(domain) );
	str = str+strlen(domain);
	*str = '#';
	str++;
	*str = '\0';
	//Now return the result and free the alloc before that
	pfree(local);
	pfree(domain);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(complex_send);
Datum email_send(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	Email *input = (Email *) PG_GETARG_POINTER(0);
	int i;
	char *str = (char*)palloc0(260);
	char *temp = input->data;
	i=0;
	while(i<260){
	  str[i]=temp[i];
	  i++;	
	}	
	str[259]='\0';

	pq_begintypsend(&buf);
	pq_sendstring(&buf, str);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*****************************************************************************
 * New Operators
 *****************************************************************************/
PG_FUNCTION_INFO_V1(email_eq); //Email1 = Email2
Datum email_eq(PG_FUNCTION_ARGS)
{
	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';	
	
	//Investigate on the boolean value of postgree
	PG_RETURN_BOOL( (strcmp(aLocal, bLocal) == 0) && (strcmp(aDomain, bDomain) == 0) );
}

PG_FUNCTION_INFO_V1(email_not_eq); //Email1 <> Email2

Datum
email_not_eq(PG_FUNCTION_ARGS)
{
	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of inputresult = 0;
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;	
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';		
		
	//Investigate on the boolean value of postgree
	PG_RETURN_BOOL(!((strcmp(aLocal, bLocal) == 0) && (strcmp(aDomain, bDomain) == 0)));
}

PG_FUNCTION_INFO_V1(email_gt); //Email1 > Email2
Datum email_gt(PG_FUNCTION_ARGS)
{
	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;
	int i;	

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;	
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';		

	//Compare function
	i = strcmp(aDomain, bDomain);
	if (i > 0) {
		PG_RETURN_BOOL(1);
	}
	else if (i < 0) {
		PG_RETURN_BOOL(0);
	}
	else {
		if (strcmp(aLocal, bLocal) > 0) {
			PG_RETURN_BOOL(1);
		}
		else {
			PG_RETURN_BOOL(0);
		}
	}
}

PG_FUNCTION_INFO_V1(email_gt_eq); //Email1 >= Email2

Datum
email_gt_eq(PG_FUNCTION_ARGS)
{
	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;	
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;
	int i;	

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;	
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';
	//Compare function
	i = strcmp(aDomain, bDomain);
	if (i > 0) {
		PG_RETURN_BOOL(1);
	}
	else if (i < 0) {
		PG_RETURN_BOOL(0);
	}
	else {
		if (strcmp(aLocal, bLocal) >= 0) {
			PG_RETURN_BOOL(1);
		}
		else {
			PG_RETURN_BOOL(0);
		}
	}
}

PG_FUNCTION_INFO_V1(email_lt); //Email1 < Email2
Datum
email_lt(PG_FUNCTION_ARGS)
{
	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;
	int i;	

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;	
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';

	i = strcmp(aDomain, bDomain);
	if (i < 0) {
		PG_RETURN_BOOL(1);
	}
	else if (i > 0) {
		PG_RETURN_BOOL(0);
	}
	else {
		if (strcmp(aLocal, bLocal) < 0) {
			PG_RETURN_BOOL(1);
		}
		else {
			PG_RETURN_BOOL(0);
		}
	}
}

PG_FUNCTION_INFO_V1(email_lt_eq); //Email1 <= Email2
Datum email_lt_eq(PG_FUNCTION_ARGS)
{
	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;
	int i;	

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;	
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';	

	//Compare function	
	i = strcmp(aDomain, bDomain);
	if (i < 0) {
		PG_RETURN_BOOL(1);
	}
	else if (i > 0) {
		PG_RETURN_BOOL(0);
	}
	else {
		if (strcmp(aLocal, bLocal) <= 0) {
			PG_RETURN_BOOL(1);
		}
		else {
			PG_RETURN_BOOL(0);
		}
	}
}

PG_FUNCTION_INFO_V1(email_domain_eq); //Email1 ~ Email2
Datum email_domain_eq(PG_FUNCTION_ARGS)
{
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;	
	
	aDomain = (char*) palloc0( strlen(a->data) - (aLenghtLocal +1) +1);
	bDomain = (char*) palloc0( strlen(b->data) - (bLenghtLocal +1) +1);
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';

	PG_RETURN_BOOL(strcmp(aDomain, bDomain) == 0);
}

PG_FUNCTION_INFO_V1(email_not_domain_eq); //Email1 !~ Email2
Datum email_not_domain_eq(PG_FUNCTION_ARGS)
{
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;

	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local lenPG_RETURN_INT32(result);gth of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;	
	
	aDomain = (char*) palloc0( strlen(a->data) - (aLenghtLocal +1) +1);
	bDomain = (char*) palloc0( strlen(b->data) - (bLenghtLocal +1) +1);

	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';

	PG_RETURN_BOOL(!(strcmp(aDomain, bDomain) == 0));
}



PG_FUNCTION_INFO_V1(email_cmp);
Datum email_cmp(PG_FUNCTION_ARGS)
{

	char* aLocal ;
	char* bLocal ;
	char* aDomain ;
	char* bDomain ;
	char *delimiter;
	
	int aLenghtLocal;
	int aLenghtDomain;
	int bLenghtLocal;
	int bLenghtDomain;
	int32	result;
	
	Email    *a = (Email *) PG_GETARG_POINTER(0);
	Email    *b = (Email *) PG_GETARG_POINTER(1);

	//Now we need to get the local length of input
	delimiter = strchr(a->data,'@');
	aLenghtLocal = delimiter - a->data;
	delimiter = strchr(b->data,'@');
	bLenghtLocal = delimiter - b->data;	
	//And next is the domain length	
	delimiter = strchr(a->data,'#');
	aLenghtDomain = (int)(delimiter - a->data) - aLenghtLocal -1;
	delimiter = strchr(b->data,'#');
	bLenghtDomain = (int)(delimiter - b->data) - bLenghtLocal -1;
	//We need to get the local and domain of each input
	aLocal = (char*) palloc0( aLenghtLocal +1);
	bLocal = (char*) palloc0( bLenghtLocal +1);
	aDomain = (char*) palloc0( aLenghtDomain +1);
	bDomain = (char*) palloc0( bLenghtDomain +1);
	//Get the local		
	memmove(aLocal,a->data,aLenghtLocal);
	*(aLocal+aLenghtLocal+1)='t';
	*(aLocal+aLenghtLocal+2)='\0';
	memmove(bLocal,b->data,bLenghtLocal);
	*(bLocal+bLenghtLocal+1)='t';
	*(bLocal+bLenghtLocal+2)='\0';	
	//Get the domain
	memmove(aDomain,a->data + aLenghtLocal + 1,aLenghtDomain);
	*(aDomain+aLenghtDomain+1)='t';
	*(aDomain+aLenghtDomain+2)='\0';
	memmove(bDomain,b->data + bLenghtLocal + 1,bLenghtDomain);
	*(bDomain+bLenghtDomain+1)='t';
	*(bDomain+bLenghtDomain+2)='\0';

	//Compare function
	result = strcmp(aDomain, bDomain);
	if (result == 0) {
		result = strcmp(aLocal, bLocal);
	}
	
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);

	PG_RETURN_INT32(result);
}


PG_FUNCTION_INFO_V1(email_hash);
Datum
email_hash(PG_FUNCTION_ARGS)
{
	Email *email = (Email *) PG_GETARG_POINTER(0);
	Datum result;	
	char *temp;	
	char *tempRun;	
	char *str = (char *) palloc0(259);
	int len;
	len = 0;
	temp=email->data;
	tempRun=str;
	while(*temp!='#'){
		*tempRun = *temp;
		temp++;
		tempRun++;
		len++;	
	}
	*tempRun='\0';
	
        result = hash_any((unsigned char *) str, len);
	pfree(str);
	// Avoid leaking memory for toasted inputs 	
	//PG_FREE_IF_COPY(email, 0);
	PG_RETURN_DATUM(result);
}


