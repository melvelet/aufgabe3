#include <hubDB/DBMyQueryManager.h>

using namespace HubDB::Exception;
using namespace HubDB::Manager;
using namespace HubDB::Table;
using namespace HubDB::Index;

LoggerPtr DBMyQueryManager::logger(Logger::getLogger("HubDB.Query.DBMyQueryManager"));

int rMyQMgr = DBMyQueryManager::registerClass();

extern "C" void *createDBMyQueryMgr(int nArgs, va_list ap);

DBMyQueryManager::DBMyQueryManager(DBServerSocket &socket, DBSysCatMgr &sysCatMgr) :
        DBQueryMgr(socket, sysCatMgr) {
  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBMyQueryMgr()");
  }

  // TODO Code hier einfügen

}

/**
 * Destruktor
 */
DBMyQueryManager::~DBMyQueryManager() {
  LOG4CXX_INFO(logger, "~DBMyQueryManager()");

  // TODO Code hier einfügen
}

string DBMyQueryManager::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBMyQueryManager]" << endl;

  // TODO Code hier einfügen

  ss << linePrefix << "----------------" << endl;
  return ss.str();
}

//R: duplicate of old method
void DBMyQueryManager::selectJoinTupleNested(DBTable *table[2],
                                       uint attrJoinPos[2],
                                       DBListPredicate where[2],
                                       DBListJoinTuple &tuples) {
  LOG4CXX_INFO(logger, "selectJoinTuple()");

  DBListTuple l[2];
  for (uint i = 0; i < 2; ++i) {
    selectTuple(table[i], where[i], l[i]);
    // cout << "------------------ table " << i;
    // cout << table[i]->getRelDef().toString();
    // cout << attrJoinPos[i];
  }
  DBListTuple::iterator i = l[0].begin();
  while (i != l[0].end()) {
    DBTuple &left = (*i);

    DBListTuple::iterator u = l[1].begin();
    while (u != l[1].end()) {
      DBTuple &right = (*u);

      if (left.getAttrVal(attrJoinPos[0]) == right.getAttrVal(attrJoinPos[1])) {
        LOG4CXX_DEBUG(logger, "left:\n" + left.toString("\t"));
        LOG4CXX_DEBUG(logger, "right:\n" + right.toString("\t"));
        pair<DBTuple, DBTuple> p;
        p.first = left;
        p.second = right;
        tuples.push_back(p);
      }
      ++u;
    }
    ++i;
  }
}

/**
 * Diese Methode bestimmt Paare von Join-Partner-Tupeln und
 * füllt die übergegebene Tupelpaar-Liste mit diesen. Jedes Paar
 * enthä̈lt jeweils ein Tupel aus jeder der beiden angegebenen
 * Tabellen. Die einzelnen Partner-Tupel mü̈ssen jeweils für
 * sich alle Selektionsprädikate der für ihre Tabelle übergegebenen Liste
 * erfüllen. Weiterhin müssen alle Partner-Tupel bei den jeweils
 * angegebenen Attributen dieselben Werte besitzen.
 * @param table
 * @param attrJoinPos
 * @param where
 * @param tuples
 */
void DBMyQueryManager::selectJoinTuple(DBTable *table[2],
                                       uint attrJoinPos[2],
                                       DBListPredicate where[2],
                                       DBListJoinTuple &tuples) {
  LOG4CXX_INFO(logger, "selectJoinTuple()");

  // Determine outer-inner relation
  const DBRelDef &def0 = table[0]->getRelDef();
  const DBRelDef &def1 = table[1]->getRelDef();
  DBAttrDef adef0 = def0.attrDef(attrJoinPos[0]);
  DBAttrDef adef1 = def0.attrDef(attrJoinPos[1]);
  uint smaller = 0;
  if (table[0]->getPageCnt() > table[1]->getPageCnt()) {
    smaller = 1;
  }

  bool indexed = false;
  uint outer = 0;
  if (adef0.isIndexed()) {
    indexed = true;
    if (adef1.isIndexed()) {
      if (smaller == 1) {
        outer = 1;
      } 
    } else {
        outer = 1;
    } // else outer = 0
  } else if (adef1.isIndexed()) {
    indexed = true;
  } else {
    if (smaller == 1) {
      outer = 1;
    }
  }

  // cout << "indexed: " << indexed << "    outer: " << outer << "    smaller: " << smaller << "\n";

  if (!indexed) {
    selectJoinTupleNested(table, attrJoinPos, where, tuples);
    // cout << "not indexed\n";
  } else {
    // cout << "Indexed\n";
    selectJoinTupleIndexedNested(table, attrJoinPos, where, tuples, outer);
  }

  

}

void DBMyQueryManager::selectJoinTupleIndexedNested(DBTable *table[2],
                                       uint attrJoinPos[2],
                                       DBListPredicate where[2],
                                       DBListJoinTuple &tuples,
                                       uint outer) {

  uint inner = 1 - outer;

  DBListTuple outerlist;
  DBListTuple innerlist;

  selectTuple(table[outer], where[outer], outerlist);
  // cout << "size: " << outerlist.size() << "\n";
  

  DBListTuple::iterator i = outerlist.begin();
  while (i != outerlist.end()) {
    DBTuple &left = (*i);

    // const DBAttrType attrValue = left.getAttrVal(attrJoinPos[outer]);
    selectIndexedTuple(table[inner], where[inner], innerlist,
                       attrJoinPos[inner], left.getAttrVal(attrJoinPos[outer]));
    // cout << "size: " << innerlist.size() << "\n";

    DBListTuple::iterator u = innerlist.begin();
    while (u != innerlist.end()) {
      DBTuple &right = (*u);
      LOG4CXX_DEBUG(logger, "left:\n" + left.toString("\t"));
      LOG4CXX_DEBUG(logger, "right:\n" + right.toString("\t"));
      pair<DBTuple, DBTuple> p;
      p.first = left;
      p.second = right;
      tuples.push_back(p);
      ++u;
    }
    ++i;
  }
}

/**
 * Diese Methode selektiert aus der angegebenen Tabelle alle Tupel, welche
 * alle Selektionsprädikate in der übergegebene Liste erfüllen.
 * Mit den selektierten Tupel wird die übergegebene Tupelliste gefüllt.
 * @param table
 * @param where
 * @param tuple
 */
void DBMyQueryManager::selectTuple(DBTable *table,
        DBListPredicate &where,
        DBListTuple &tuple) {
  LOG4CXX_INFO(logger, "selectTuple()");
  LOG4CXX_DEBUG(logger, "table:\n" + table->toString("\t"));
  LOG4CXX_DEBUG(logger, "where: " + TO_STR(where));

  DBListTuple l;
  TID t;
  t.page = 0;
  t.slot = 0;
  list<uint> posList;
  list<bool> checkList;
  DBListTID tidList;
  const DBRelDef &def = table->getRelDef();
  QualifiedName qname;
  bool indexUsed = false;

  strcpy(qname.relationName, def.relationName().c_str());

  DBListPredicate::iterator u = where.begin();
  while (u != where.end()) {
    DBPredicate &p = *u;
    if (strcmp(def.relationName().c_str(), p.name().relationName) != 0)
      throw DBQueryMgrException("Predicate missmatch");
    DBAttrDef adef = def.attrDef(p.name().attributeName);
    if (adef.isIndexed() == true) {
      checkList.push_back(false);
      strcpy(qname.attributeName, adef.attrName().c_str());
      DBListTID tidListTmp;
      DBIndex *index = NULL;
      try {
        index = sysCatMgr.openIndex(connectDB, qname, READ);
        if (indexUsed == true) {
          index->find(p.val(), tidListTmp);
          tidListTmp.sort();
        } else {
          index->find(p.val(), tidList);
          tidList.sort();
        }
        delete index;
      } catch (DBException e) {
        if (index != NULL)
          delete index;
        throw e;
      }
      if (indexUsed == true) {
        DBListTID tidListNew;
        set_intersection(tidList.begin(), tidList.end(), tidListTmp.begin(), tidListTmp.end(),
                         std::inserter(tidListNew, tidListNew.begin()));
        tidList = tidListNew;
      } else {
        indexUsed = true;
      }
      LOG4CXX_DEBUG(logger, "tidList: " + TO_STR(tidList));
      if (tidList.size() == 0)
        break;
    } else {
      checkList.push_back(true);
    }
    posList.push_back(adef.attrPos());
    ++u;
  }

  do {
    l.clear();
    if (indexUsed == true) {
      table->readTIDs(tidList, l);
    } else {
      t = table->readSeqFromTID(t, 100, l);
    }
    LOG4CXX_DEBUG(logger, "read " + TO_STR(l.size()) + " tuples");
    DBListTuple::iterator i = l.begin();
    while (l.end() != i) {
      bool match = true;
      u = where.begin();
      list<uint>::iterator ii = posList.begin();
      list<bool>::iterator ic = checkList.begin();
      while (match == true && u != where.end()) {
        DBPredicate &p = *u;
        if (*ic == true &&
            !(p.val() == (*i).getAttrVal(*ii)))
          match = false;
        ++ii;
        ++ic;
        ++u;
      }
      if (match == true) {
        LOG4CXX_DEBUG(logger, "tuple: " + (*i).toString("\t"));
        tuple.push_back(*i);
      }
      ++i;
    }
  } while (l.size() == 100 && indexUsed == false);
  LOG4CXX_DEBUG(logger, "return");
}


void DBMyQueryManager::selectIndexedTuple(DBTable *table,
        DBListPredicate &where,
        DBListTuple &tuple,
        uint attrIndex,
        const DBAttrType &attrValue) {
  LOG4CXX_INFO(logger, "selectTuple()");
  LOG4CXX_DEBUG(logger, "table:\n" + table->toString("\t"));
  LOG4CXX_DEBUG(logger, "where: " + TO_STR(where));

  tuple.clear();
  DBListTuple l;
  TID t;
  t.page = 0;
  t.slot = 0;
  list<uint> posList;
  list<bool> checkList;
  DBListTID tidList;
  const DBRelDef &def = table->getRelDef();
  QualifiedName qname;
  strcpy(qname.relationName, def.relationName().c_str());

  // cout << def.toString();
  DBIndex *index = NULL;
  DBAttrDef adef = def.attrDef(attrIndex);
  strcpy(qname.attributeName, adef.attrName().c_str());
  try {
    index = sysCatMgr.openIndex(connectDB, qname, READ);
    index->find(attrValue, tidList);
    tidList.sort();
    delete index;
  } catch (DBException e) {
    if (index != NULL)
      delete index;
    throw e;
  }

  DBListPredicate::iterator u = where.begin();
  while (u != where.end()) {
    DBPredicate &p = *u;
    if (strcmp(def.relationName().c_str(), p.name().relationName) != 0)
      throw DBQueryMgrException("Predicate missmatch");
    DBAttrDef adef = def.attrDef(p.name().attributeName);
    if (adef.isIndexed() == true) {
      checkList.push_back(false);
      strcpy(qname.attributeName, adef.attrName().c_str());
      DBListTID tidListTmp;
      DBIndex *index = NULL;
      try {
        index = sysCatMgr.openIndex(connectDB, qname, READ);

        index->find(p.val(), tidListTmp);
        tidListTmp.sort();

        delete index;
      } catch (DBException e) {
        if (index != NULL)
          delete index;
        throw e;
      }

      DBListTID tidListNew;
      set_intersection(tidList.begin(), tidList.end(), tidListTmp.begin(), tidListTmp.end(),
                        std::inserter(tidListNew, tidListNew.begin()));
      tidList = tidListNew;

      LOG4CXX_DEBUG(logger, "tidList: " + TO_STR(tidList));
      if (tidList.size() == 0)
        break;
    } else {
      checkList.push_back(true);
    }
    posList.push_back(adef.attrPos());
    ++u;
  }

  do {
    l.clear();

    table->readTIDs(tidList, l);

    LOG4CXX_DEBUG(logger, "read " + TO_STR(l.size()) + " tuples");
    DBListTuple::iterator i = l.begin();
    while (l.end() != i) {
      bool match = true;
      u = where.begin();
      list<uint>::iterator ii = posList.begin();
      list<bool>::iterator ic = checkList.begin();
      while (match == true && u != where.end()) {
        DBPredicate &p = *u;
        if (*ic == true &&
            !(p.val() == (*i).getAttrVal(*ii)))
          match = false;
        ++ii;
        ++ic;
        ++u;
      }
      if (match == true) {
        LOG4CXX_DEBUG(logger, "tuple: " + (*i).toString("\t"));
        tuple.push_back(*i);
      }
      ++i;
    }
  } while (l.size() == 100);
  LOG4CXX_DEBUG(logger, "return");
}

/**
 * Fügt createDBMyQueryMgr zur globalen factory method-map hinzu
 */
int DBMyQueryManager::registerClass() {
  setClassForName("DBMyQueryManager", createDBMyQueryMgr);
  return 0;
}

/**
 * Wird aufgerufen von HubDB::Types::getClassForName von DBTypes,
 * um QueryManager zu erstellen
 *
 */
extern "C" void *createDBMyQueryMgr(int nArgs, va_list ap) {
  if (nArgs != 2) {
    throw DBException("Invalid number of arguments");
  }
  DBServerSocket *socket = va_arg(ap, DBServerSocket *);
  DBSysCatMgr *sysCat = va_arg(ap, DBSysCatMgr *);
  return new DBMyQueryManager(*socket, *sysCat);
}