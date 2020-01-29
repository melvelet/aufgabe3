#ifndef HUBDB_DBMYQUERYMANAGER_H
#define HUBDB_DBMYQUERYMANAGER_H

#include <hubDB/DBQueryMgr.h>

namespace HubDB {
    namespace Manager {
        class DBMyQueryManager : public DBQueryMgr {
        public:
            DBMyQueryManager(DBServerSocket & socket,DBSysCatMgr & sysCatMgr);
            ~DBMyQueryManager();
            string toString(string linePrefix="") const;

            void selectTuple(DBTable * table,DBListPredicate & where,DBListTuple & tuple);
            void selectIndexedTuple(DBTable * table,DBListPredicate & where,DBListTuple & tuple);
            void selectJoinTuple(DBTable * table[2],uint attrJoinPos[2],DBListPredicate where[2],DBListJoinTuple & tuples);
            void selectJoinTupleNested(DBTable * table[2],uint attrJoinPos[2],DBListPredicate where[2],DBListJoinTuple & tuples);
            void selectJoinTupleIndexedNested(DBTable * table[2],uint attrJoinPos[2],DBListPredicate where[2],DBListJoinTuple & tuples, bool indexed, uint outer);
            DBIndex* createTempIndex(DBListTuple &tuplelist, uint attrJoinPos);

            static int registerClass();

        private:
            void createIndex(const QualifiedName & qname,char * type) {
                DBQueryMgr::createIndex(qname, type);
        }
            static LoggerPtr logger;
        };
    }
}

#endif //HUBDB_DBMYQUERYMANAGER_H
