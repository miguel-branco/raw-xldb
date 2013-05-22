#include "common-macros.h"

#include "supersonic/supersonic.h"
using supersonic::Table;
using supersonic::TupleSchema;
using supersonic::Attribute;
using supersonic::HeapBufferAllocator;
using supersonic::kRowidDatatype;
using supersonic::INT32;
using supersonic::UINT32;
using supersonic::INT64;
using supersonic::UINT64;
using supersonic::FLOAT;
using supersonic::DOUBLE;
using supersonic::BOOL;
using supersonic::NOT_NULLABLE;
using supersonic::NULLABLE;
using supersonic::rowid_t;
using supersonic::Expression;
using supersonic::NamedAttribute;
using supersonic::ConstFloat;
using supersonic::ConstInt32;
using supersonic::Abs;
using supersonic::Plus;
using supersonic::Multiply;
using supersonic::And;
using supersonic::Or;
using supersonic::Equal;
using supersonic::NotEqual;
using supersonic::Less;
using supersonic::LessOrEqual;
using supersonic::Greater;
using supersonic::GreaterOrEqual;
using supersonic::CompoundSingleSourceProjector;
using supersonic::CompoundMultiSourceProjector;
using supersonic::ProjectNamedAttribute;
using supersonic::Operation;
using supersonic::Filter;
using supersonic::HashJoinOperation;
using supersonic::INNER;
using supersonic::ANTI;
using supersonic::UNIQUE;
using supersonic::FailureOrOwned;
using supersonic::Cursor;
using supersonic::ResultView;
using supersonic::View;
using supersonic::rowcount_t;
using supersonic::GroupAggregate;
using supersonic::AggregationSpecification;
using supersonic::COUNT;
#include "supersonic/cursor/core/coalesce.h"
using supersonic::Coalesce;
#include "supersonic/cursor/core/merge_union_all.h"
using supersonic::MergeUnionAll;

#include <iostream>
using std::cout;
using std::endl;
#include <time.h>

double
SupersonicBenchmarkDiff(struct timespec st, struct timespec end)
{
  struct timespec tmp;

  if ((end.tv_nsec - st.tv_nsec) < 0) {
    tmp.tv_sec = end.tv_sec - st.tv_sec - 1;
    tmp.tv_nsec = 1e9 + end.tv_nsec - st.tv_nsec;
  } else {
    tmp.tv_sec = end.tv_sec - st.tv_sec;
    tmp.tv_nsec = end.tv_nsec - st.tv_nsec;
  }

  return tmp.tv_sec + tmp.tv_nsec * 1e-9;
}

void SupersonicPrintHelper(FailureOrOwned<Cursor> cursor) {
  if (!cursor.is_success()) {
    throw new RawException(cursor.exception().message());
  }

  int first = 1;
  while (1) {
    ResultView result(cursor->Next(-1));
    if (!result.has_data()) {
      break;
    }

    View result_view(result.view());

    if (first) {
      for (int col = 0; col < result_view.schema().attribute_count(); col++) {
        cout << result_view.schema().attribute(col).name();
        if (col < result_view.schema().attribute_count() - 1) {
          cout << "\t";
        }
      }
      cout << endl;
      first = 0;
    }

    for (rowcount_t row = 0; row < result_view.row_count(); row++) {
      for (int col = 0; col < result_view.schema().attribute_count(); col++) {
        if (result_view.column(col).is_null() != NULL && result_view.column(col).is_null()[row]) {
          cout << "<NULL>";
        } else {
          switch (result_view.schema().attribute(col).type()) {
          case INT32:
            cout << result_view.column(col).typed_data<INT32>()[row];
            break;
          case UINT32:
            cout << result_view.column(col).typed_data<UINT32>()[row];
            break;
          case INT64:
            cout << result_view.column(col).typed_data<INT64>()[row];
            break;
          case UINT64:
            cout << result_view.column(col).typed_data<UINT64>()[row];
            break;
          case FLOAT:
            cout << result_view.column(col).typed_data<FLOAT>()[row];
            break;
          case DOUBLE:
            cout << result_view.column(col).typed_data<DOUBLE>()[row];
            break;
          case BOOL:
            cout << (result_view.column(col).typed_data<BOOL>()[row] ? "T" : "F");
            break;
          default:
            abort();
          }
        }
        if (col < result_view.schema().attribute_count() - 1) {
          cout << "\t";
        }
      }
      cout << endl;
    }
  }
}

#define SUPERSONIC_SCHEMA_NEW(id)                         TupleSchema schema_##id;
#define SUPERSONIC_SCHEMA_ADD(id,name,type,nullable)      schema_##id.add_attribute(Attribute(#name,type,nullable));
#define SUPERSONIC_TABLE_NEW(id)                          Table* table_##id = new Table(schema_##id, HeapBufferAllocator::Get()); \
                                                          scoped_ptr<Operation> id(table_##id);
#define SUPERSONIC_TABLE_ADD(id)                          rowid_t row = table_##id->AddRow();
#define SUPERSONIC_TABLE_SET(id,col,type,value)           table_##id->Set<type>(col, row, value);
#define SUPERSONIC_TABLE_SET_KEY(id,col,var)              table_##id->Set<kRowidDatatype>(col, row, var);
#define SUPERSONIC_PROJECTOR_NEW(id)                      scoped_ptr<CompoundSingleSourceProjector> projector_##id( \
                                                            new CompoundSingleSourceProjector());
#define SUPERSONIC_PROJECTOR_ADD(id,name)                 projector_##id->add(ProjectNamedAttribute(#name));
#define SUPERSONIC_FILTER(id,expr,child)                  scoped_ptr<const Expression> expression_##id(expr); \
                                                          scoped_ptr<Operation> id(                           \
                                                            Filter(expression_##id.release(),                 \
                                                                   projector_##id.release(),                  \
                                                                   child.release()));
#define SUPERSONIC_HASH_JOIN(id,left,right,inner,unique)  scoped_ptr<CompoundMultiSourceProjector> projector_result_##id( \
                                                            (new CompoundMultiSourceProjector())                          \
                                                            ->add(0, projector_lhs_##id.release())                        \
                                                            ->add(1, projector_rhs_##id.release()));                      \
                                                          scoped_ptr<Operation> id(                                       \
                                                            new HashJoinOperation(inner,                                  \
                                                            projector_lhs_selector_##id.release(),                        \
                                                            projector_rhs_selector_##id.release(),                        \
                                                            projector_result_##id.release(),                              \
                                                            unique,                                                       \
                                                            left.release(),                                               \
                                                            right.release()));
#define SUPERSONIC_AGGREGATOR_NEW(id)                     scoped_ptr<AggregationSpecification> agg_spec_##id( \
                                                            new AggregationSpecification());
#define SUPERSONIC_AGGREGATOR_ADD(id,func,field,alias)    agg_spec_##id->AddAggregation(func, #field, #alias);
#define SUPERSONIC_GROUP_AGGREGATE(id,child)              scoped_ptr<Operation> id(                           \
                                                            GroupAggregate(projector_agg_key_##id.release(),  \
                                                            agg_spec_##id.release(),                          \
                                                            NULL,                                             \
                                                            child.release()));                                 
#define SUPERSONIC_UNION(id,child)                                    FailureOrOwned<Cursor> cursor_##child = child.release()->CreateCursor();        \
                                                                      if (!cursor_##child.is_success()) {                                             \
                                                                        throw new RawException("cursor.is_success");                                  \
                                                                      }                                                                               \
                                                                      while (1) {                                                                     \
                                                                        ResultView result(cursor_##child->Next(-1));                                  \
                                                                        if (!result.has_data()) {                                                     \
                                                                          break;                                                                      \
                                                                        }                                                                             \
                                                                        View result_view(result.view());                                              \
                                                                        table_##id->AppendView(result_view);                                          \
                                                                      }
#define SUPERSONIC_COLUMN_LOOP_BEGIN(id,child,selector,stype,ctype)   FailureOrOwned<Cursor> cursor_##id = child.release()->CreateCursor();           \
                                                                      if (!cursor_##id.is_success()) {                                                \
                                                                        throw new RawException("cursor.is_success");                                  \
                                                                      }                                                                               \
                                                                      while (1) {                                                                     \
                                                                        ResultView result(cursor_##id->Next(-1));                                     \
                                                                        if (!result.has_data()) {                                                     \
                                                                          break;                                                                      \
                                                                        }                                                                             \
                                                                        View result_view(result.view());                                              \
                                                                        int col;                                                                      \
                                                                        for (int i = 0; i < result_view.schema().attribute_count(); i++) {            \
                                                                          if (result_view.schema().attribute(i).name() == #selector) {                \
                                                                            col = i;                                                                  \
                                                                          }                                                                           \
                                                                        }                                                                             \
                                                                        for (rowid_t idx_##id = 0; idx_##id < result_view.row_count(); idx_##id++) {  \
                                                                          ctype id = result_view.column(col).typed_data<stype>()[idx_##id];
#define SUPERSONIC_COLUMN_LOOP_END()                                    }                                                                             \
                                                                      }
#define SUPERSONIC_STORE(id,child)                        scoped_ptr<Operation> id(child.release());
#define SUPERSONIC_REUSE(id,child)                        scoped_ptr<Operation> id(child.get());
#define SUPERSONIC_STORE_RELEASE(id)                      id.release();
#define SUPERSONIC_BENCHMARK_START(id)                    struct timespec start_##id, end_##id;                           \
                                                          clock_gettime(CLOCK_REALTIME, &start_##id);
#define SUPERSONIC_BENCHMARK_STOP(id)                     clock_gettime(CLOCK_REALTIME, &end_##id);
#define SUPERSONIC_BENCHMARK_PRINT(id)                    cout << "[" << #id << "] Time elapsed: " << SupersonicBenchmarkDiff(start_##id, end_##id) << endl;
#define SUPERSONIC_PRINT(id)                              SupersonicPrintHelper(id.release()->CreateCursor());


#define LOAD_FILES_LOOP_BEGIN(id,n,files...)              char* array_##id[] = {files};               \
                                                          for (int i = 0; i < n; i++) {               \
                                                            char* id = array_##id[i];
#define LOAD_FILES_LOOP_END(id,child)                       SUPERSONIC_UNION(id,child)                \
                                                          }
                                                                    
