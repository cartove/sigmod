/*
* core.cpp version 1.0
* Copyright (c) 2013 KAUST - InfoCloud Group (All Rights Reserved)
* Author: Amin Allam
*
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
* OTHER DEALINGS IN THE SOFTWARE.
*/

#include "../include/core.h"

#include "thpool.c"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <vector>

#include <stdlib.h>
using namespace std;

///////////////////////////////////////////////////////////////////////////////////////////////
// EditDistance ()computes the edit distance between 2 strings s1 and s2
// each is follwed by it's size m , n
// both are C style strings and the algorithm is DP
// min3() is an optimized instructions to get min of three intgers
// min3() have a significant effect it can be optimized further
long int inline min3 (long int a, long int b, long int c)
{
    __asm__ (
    "cmp     %0, %1\n\t"
    "cmovle  %1, %0\n\t"
    "cmp     %0, %2\n\t"
    "cmovle  %2, %0\n\t"
    : "+r"(a) :
        "%r"(b), "r"(c)
      );
    return a;
}

long int EditDistance(char* s1,  int m, char* s2,  int n)
{
     int table[m][n];
    for (long int i = 0; i <= m; ++i)
        table[i][0] = i;
    for ( long int i = 0; i <= n; ++i)
        table[0][i] = i;
    for ( long int  i = 1; i <= m; ++i) {
        for ( int j = 1; j <= n; ++j) {
            if (s1[i-1] == s2[j-1]) table[i][j]  = table[i-1][j-1];
            else table[i][j] = 1 + min3(table[i][j-1],table[i-1][j],table[i-1][j-1]);
        }
    }
    return table[m][n];
}


///////////////////////////////////////////////////////////////////////////////////////////////

// Computes Hamming distance between a null-terminated string "a" with length "na"
//  and a null-terminated string "b" with length "nb"
unsigned int HammingDistance(char* a, int na, char* b, int nb)
{
  int j, oo=0x7FFFFFFF;
  if(na!=nb) return oo;

  unsigned int num_mismatches=0;
  for(j=0;j<na;j++){
      if(a[j]!=b[j]) num_mismatches++;
      if ( num_mismatches > 3)
        return num_mismatches;

    }
}

///////////////////////////////////////////////////////////////////////////////////////////////
struct Query {
  QueryID query_id;
  char str[MAX_QUERY_LENGTH];
  MatchType match_type;
  unsigned int match_dist;
  int query_result;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all query ID results associated with a dcoument
struct Document
{
  DocID doc_id;
  unsigned int num_res;
  QueryID* query_ids;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all currently active queries
vector<Query> queries;

// Keeps all currently available results that has not been returned yet
vector<Document> docs;

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){return EC_SUCCESS;}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){return EC_SUCCESS;}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist)
{
  Query query;
  query.query_id=query_id;
  strcpy(query.str, query_str);
  query.match_type=match_type;
  query.match_dist=match_dist;
  // Add this query to the active query set
  queries.push_back(query);
  return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode EndQuery(QueryID query_id)
{
  // Remove this query from the active query set
  unsigned int i, n=queries.size();
  for(i=0;i<n;i++)
    {
      if(queries[i].query_id==query_id)
        {
          queries.erase(queries.begin()+i);
          break;
        }
    }
  return EC_SUCCESS;
}

struct query_thread_prams{
char * cur_doc_str;
Query * quer;
query_thread_prams (char *doc_str ,Query query){
  strcpy(this->cur_doc_str,doc_str);
  this->quer=new Query(query);
}
};
vector<unsigned int> query_ids;

void * QueryLOL(char * cur_doc_str  , Query &quer){
  bool matching_query=true;
  int iq=0;
  while(quer.str[iq] && matching_query)
    {
      while(quer.str[iq]==' ') iq++;
      if(!quer.str[iq]) break;
      char* qword=&quer.str[iq];

      int lq=iq;
      while(quer.str[iq] && quer.str[iq]!=' ') iq++;
      char qt=quer.str[iq];
      quer.str[iq]=0;
      lq=iq-lq;

      bool matching_word=false;

      int id=0;
      while(cur_doc_str[id] && !matching_word)
        {
          while(cur_doc_str[id]==' ') id++;
          if(!cur_doc_str[id]) break;
          char* dword=&cur_doc_str[id];

          int ld=id;
          while(cur_doc_str[id] && cur_doc_str[id]!=' ') id++;
          char dt=cur_doc_str[id];
          cur_doc_str[id]=0;

          ld=id-ld;

          if(quer.match_type==MT_EXACT_MATCH)
            {
              if(strcmp(qword, dword)==0) matching_word=true;
            }
          else if(quer.match_type==MT_HAMMING_DIST)
            {
              unsigned int num_mismatches=HammingDistance(qword, lq, dword, ld);
              if(num_mismatches<=quer.match_dist) matching_word=true;
            }
          else if(quer.match_type==MT_EDIT_DIST)
            {
              unsigned int edit_dist=EditDistance(qword, lq, dword, ld);
              if(edit_dist<=quer.match_dist) matching_word=true;
            }

          cur_doc_str[id]=dt;
        }

      quer.str[iq]=qt;

      if(!matching_word)
        {
          // This query has a word that does not match any word in the document
          matching_query=false;
        }
    }
  if(matching_query)
    {
      // This query matches the document
      query_ids.push_back (quer.query_id);
    }
}


ErrorCode MatchDocument(DocID doc_id, const char* doc_str)
{
  thpool_t* threadpool;
  //threadpool=thpool_init(12);

  char cur_doc_str[MAX_DOC_LENGTH];
  strcpy(cur_doc_str, doc_str);
  unsigned int i;

  // Iterate on all active queries to compare them with this new document
  for(Query quer:queries ) {
      QueryLOL(cur_doc_str,quer);
    }
  Document doc;
  doc.doc_id=doc_id;
  doc.num_res=query_ids.size();
  doc.query_ids=0;
  if(doc.num_res) doc.query_ids=(unsigned int*)malloc(doc.num_res*sizeof(unsigned int));
  for(i=0;i<doc.num_res;i++) doc.query_ids[i]=query_ids[i];
  // Add this result to the set of undelivered results
  docs.push_back(doc);
  query_ids.clear ();
  return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
  // Get the first undeliverd resuilt from "docs" and return it
  *p_doc_id=0; *p_num_res=0; *p_query_ids=0;
  if(docs.size()==0) return EC_NO_AVAIL_RES;
  *p_doc_id=docs[0].doc_id; *p_num_res=docs[0].num_res; *p_query_ids=docs[0].query_ids;
  docs.erase(docs.begin());
  return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////
