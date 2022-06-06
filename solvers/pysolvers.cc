/*
 * pysolvers.cc
 *
 *  Created on: Nov 26, 2016
 *      Author: aign
 */

#ifdef _MSC_VER
#define setlinebuf(a)
#endif

#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <vector>

#ifdef WITH_CADICAL
#include "cadical/cadical.hpp"
#endif

#ifdef WITH_GLUECARD30
#include "gluecard30/core/Solver.h"
#endif

#ifdef WITH_GLUECARD41
#include "gluecard41/core/Solver.h"
#endif

#ifdef WITH_GLUCOSE30
#include "glucose30/core/Solver.h"
#endif

#ifdef WITH_GLUCOSE41
#include "glucose41/core/Solver.h"
#endif

#ifdef WITH_LINGELING
#include "lingeling/lglib.h"
#endif

#ifdef WITH_MAPLECHRONO
#include "maplechrono/core/Solver.h"
#endif

#ifdef WITH_MAPLECM
#include "maplecm/core/Solver.h"
#endif

#ifdef WITH_MAPLESAT
#include "maplesat/core/Solver.h"
#endif

#ifdef WITH_MERGESAT3
#include "mergesat3/core/Solver.h"
#endif

#ifdef WITH_MINICARD
#include "minicard/core/Solver.h"
#endif

#ifdef WITH_MINISAT22
#include "minisat22/core/Solver.h"
#endif

#ifdef WITH_MINISATGH
#include "minisatgh/core/Solver.h"
#endif

using namespace std;

// docstrings
//=============================================================================
static char    module_docstring[] = "This module provides a wrapper interface "
				    "for several SAT solvers.";
static char       new_docstring[] = "Create a new solver object.";
static char     addcl_docstring[] = "Add a clause to formula.";
static char     addam_docstring[] = "Add an atmost constraint to formula "
				    "(for Minicard only).";
static char     solve_docstring[] = "Solve a given CNF instance.";
static char       lim_docstring[] = "Solve a given CNF instance within a budget.";
static char      prop_docstring[] = "Propagate a given set of literals.";
static char    phases_docstring[] = "Set variable polarities.";
static char   cbudget_docstring[] = "Set limit on the number of conflicts.";
static char   pbudget_docstring[] = "Set limit on the number of propagations.";
static char interrupt_docstring[] = "Interrupt SAT solver execution (not supported by lingeling and CaDiCaL).";
static char  clearint_docstring[] = "Clear interrupt indicator flag.";
static char   setincr_docstring[] = "Set incremental mode (for Glucose3 only).";
static char   tracepr_docstring[] = "Trace resolution proof.";
static char      core_docstring[] = "Get an unsatisfiable core if formula is UNSAT.";
static char     model_docstring[] = "Get a model if formula is SAT.";
static char     nvars_docstring[] = "Get number of variables used by the solver.";
static char      ncls_docstring[] = "Get number of clauses used by the solver.";
static char       del_docstring[] = "Delete a previously created solver object.";
static char  acc_stat_docstring[] = "Get accumulated stats from the solver.";

static PyObject *SATError;
static jmp_buf env;

// function declaration for functions available in module
//=============================================================================
extern "C" {
#ifdef WITH_CADICAL
	static PyObject *py_cadical_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_cadical_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_GLUECARD30
	static PyObject *py_gluecard3_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_add_am    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_setincr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard3_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_GLUECARD41
	static PyObject *py_gluecard41_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_add_am    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_setincr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_gluecard41_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_GLUCOSE30
	static PyObject *py_glucose3_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_setincr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose3_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_GLUCOSE41
	static PyObject *py_glucose41_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_setincr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_glucose41_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_LINGELING
	static PyObject *py_lingeling_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_lingeling_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MAPLECHRONO
	static PyObject *py_maplechrono_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplechrono_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MAPLECM
	static PyObject *py_maplecm_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplecm_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MAPLESAT
	static PyObject *py_maplesat_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_tracepr   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_maplesat_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MERGESAT3
	static PyObject *py_mergesat3_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_mergesat3_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MINICARD
	static PyObject *py_minicard_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_add_am    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minicard_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MINISAT22
	static PyObject *py_minisat22_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisat22_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
#ifdef WITH_MINISATGH
	static PyObject *py_minisatgh_new       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_add_cl    (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_solve     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_solve_lim (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_propagate (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_setphases (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_cbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_pbudget   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_interrupt (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_clearint  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_core      (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_model     (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_nof_vars  (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_nof_cls   (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_del       (PyObject *, PyObject *, PyObject *);
	static PyObject *py_minisatgh_acc_stats (PyObject *, PyObject *, PyObject *);
#endif
}

// module specification
//=============================================================================
static PyMethodDef module_methods[] = {
#ifdef WITH_CADICAL
	{ "cadical_new",       (PyCFunction) py_cadical_new,       METH_VARARGS | METH_KEYWORDS,      new_docstring },
	{ "cadical_add_cl",    (PyCFunction) py_cadical_add_cl,    METH_VARARGS | METH_KEYWORDS,    addcl_docstring },
	{ "cadical_solve",     (PyCFunction) py_cadical_solve,     METH_VARARGS | METH_KEYWORDS,    solve_docstring },
	{ "cadical_tracepr",   (PyCFunction) py_cadical_tracepr,   METH_VARARGS | METH_KEYWORDS,  tracepr_docstring },
	{ "cadical_core",      (PyCFunction) py_cadical_core,      METH_VARARGS | METH_KEYWORDS,     core_docstring },
	{ "cadical_model",     (PyCFunction) py_cadical_model,     METH_VARARGS | METH_KEYWORDS,    model_docstring },
	{ "cadical_nof_vars",  (PyCFunction) py_cadical_nof_vars,  METH_VARARGS | METH_KEYWORDS,    nvars_docstring },
	{ "cadical_nof_cls",   (PyCFunction) py_cadical_nof_cls,   METH_VARARGS | METH_KEYWORDS,     ncls_docstring },
	{ "cadical_del",       (PyCFunction) py_cadical_del,       METH_VARARGS | METH_KEYWORDS,      del_docstring },
	{ "cadical_acc_stats", (PyCFunction) py_cadical_acc_stats, METH_VARARGS | METH_KEYWORDS, acc_stat_docstring },
#endif
#ifdef WITH_GLUECARD30
	{ "gluecard3_new",       (PyCFunction) py_gluecard3_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "gluecard3_add_cl",    (PyCFunction) py_gluecard3_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "gluecard3_add_am",    (PyCFunction) py_gluecard3_add_am,    METH_VARARGS | METH_KEYWORDS,     addam_docstring },
	{ "gluecard3_solve",     (PyCFunction) py_gluecard3_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "gluecard3_solve_lim", (PyCFunction) py_gluecard3_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "gluecard3_propagate", (PyCFunction) py_gluecard3_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "gluecard3_setphases", (PyCFunction) py_gluecard3_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "gluecard3_cbudget",   (PyCFunction) py_gluecard3_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "gluecard3_pbudget",   (PyCFunction) py_gluecard3_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "gluecard3_interrupt", (PyCFunction) py_gluecard3_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "gluecard3_clearint",  (PyCFunction) py_gluecard3_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "gluecard3_setincr",   (PyCFunction) py_gluecard3_setincr,   METH_VARARGS | METH_KEYWORDS,   setincr_docstring },
	{ "gluecard3_tracepr",   (PyCFunction) py_gluecard3_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "gluecard3_core",      (PyCFunction) py_gluecard3_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "gluecard3_model",     (PyCFunction) py_gluecard3_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "gluecard3_nof_vars",  (PyCFunction) py_gluecard3_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "gluecard3_nof_cls",   (PyCFunction) py_gluecard3_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "gluecard3_del",       (PyCFunction) py_gluecard3_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "gluecard3_acc_stats", (PyCFunction) py_gluecard3_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_GLUECARD41
	{ "gluecard41_new",       (PyCFunction) py_gluecard41_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "gluecard41_add_cl",    (PyCFunction) py_gluecard41_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "gluecard41_add_am",    (PyCFunction) py_gluecard41_add_am,    METH_VARARGS | METH_KEYWORDS,     addam_docstring },
	{ "gluecard41_solve",     (PyCFunction) py_gluecard41_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "gluecard41_solve_lim", (PyCFunction) py_gluecard41_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "gluecard41_propagate", (PyCFunction) py_gluecard41_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "gluecard41_setphases", (PyCFunction) py_gluecard41_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "gluecard41_cbudget",   (PyCFunction) py_gluecard41_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "gluecard41_pbudget",   (PyCFunction) py_gluecard41_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "gluecard41_interrupt", (PyCFunction) py_gluecard41_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "gluecard41_clearint",  (PyCFunction) py_gluecard41_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "gluecard41_setincr",   (PyCFunction) py_gluecard41_setincr,   METH_VARARGS | METH_KEYWORDS,   setincr_docstring },
	{ "gluecard41_tracepr",   (PyCFunction) py_gluecard41_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "gluecard41_core",      (PyCFunction) py_gluecard41_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "gluecard41_model",     (PyCFunction) py_gluecard41_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "gluecard41_nof_vars",  (PyCFunction) py_gluecard41_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "gluecard41_nof_cls",   (PyCFunction) py_gluecard41_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "gluecard41_del",       (PyCFunction) py_gluecard41_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "gluecard41_acc_stats", (PyCFunction) py_gluecard41_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_GLUCOSE30
	{ "glucose3_new",       (PyCFunction) py_glucose3_new,       METH_VARARGS | METH_KEYWORDS,  new_docstring },
	{ "glucose3_add_cl",    (PyCFunction) py_glucose3_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "glucose3_solve",     (PyCFunction) py_glucose3_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "glucose3_solve_lim", (PyCFunction) py_glucose3_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "glucose3_propagate", (PyCFunction) py_glucose3_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "glucose3_setphases", (PyCFunction) py_glucose3_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "glucose3_cbudget",   (PyCFunction) py_glucose3_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "glucose3_pbudget",   (PyCFunction) py_glucose3_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "glucose3_interrupt", (PyCFunction) py_glucose3_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "glucose3_clearint",  (PyCFunction) py_glucose3_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "glucose3_setincr",   (PyCFunction) py_glucose3_setincr,   METH_VARARGS | METH_KEYWORDS,   setincr_docstring },
	{ "glucose3_tracepr",   (PyCFunction) py_glucose3_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "glucose3_core",      (PyCFunction) py_glucose3_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "glucose3_model",     (PyCFunction) py_glucose3_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "glucose3_nof_vars",  (PyCFunction) py_glucose3_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "glucose3_nof_cls",   (PyCFunction) py_glucose3_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "glucose3_del",       (PyCFunction) py_glucose3_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "glucose3_acc_stats", (PyCFunction) py_glucose3_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_GLUCOSE41
	{ "glucose41_new",       (PyCFunction) py_glucose41_new,       METH_VARARGS | METH_KEYWORDS,  new_docstring },
	{ "glucose41_add_cl",    (PyCFunction) py_glucose41_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "glucose41_solve",     (PyCFunction) py_glucose41_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "glucose41_solve_lim", (PyCFunction) py_glucose41_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "glucose41_propagate", (PyCFunction) py_glucose41_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "glucose41_setphases", (PyCFunction) py_glucose41_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "glucose41_cbudget",   (PyCFunction) py_glucose41_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "glucose41_pbudget",   (PyCFunction) py_glucose41_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "glucose41_interrupt", (PyCFunction) py_glucose41_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "glucose41_clearint",  (PyCFunction) py_glucose41_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "glucose41_setincr",   (PyCFunction) py_glucose41_setincr,   METH_VARARGS | METH_KEYWORDS,   setincr_docstring },
	{ "glucose41_tracepr",   (PyCFunction) py_glucose41_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "glucose41_core",      (PyCFunction) py_glucose41_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "glucose41_model",     (PyCFunction) py_glucose41_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "glucose41_nof_vars",  (PyCFunction) py_glucose41_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "glucose41_nof_cls",   (PyCFunction) py_glucose41_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "glucose41_del",       (PyCFunction) py_glucose41_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "glucose41_acc_stats", (PyCFunction) py_glucose41_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_LINGELING
	{ "lingeling_new",       (PyCFunction) py_lingeling_new,       METH_VARARGS | METH_KEYWORDS,      new_docstring },
	{ "lingeling_add_cl",    (PyCFunction) py_lingeling_add_cl,    METH_VARARGS | METH_KEYWORDS,    addcl_docstring },
	{ "lingeling_solve",     (PyCFunction) py_lingeling_solve,     METH_VARARGS | METH_KEYWORDS,    solve_docstring },
	{ "lingeling_setphases", (PyCFunction) py_lingeling_setphases, METH_VARARGS | METH_KEYWORDS,   phases_docstring },
	{ "lingeling_tracepr",   (PyCFunction) py_lingeling_tracepr,   METH_VARARGS | METH_KEYWORDS,  tracepr_docstring },
	{ "lingeling_core",      (PyCFunction) py_lingeling_core,      METH_VARARGS | METH_KEYWORDS,     core_docstring },
	{ "lingeling_model",     (PyCFunction) py_lingeling_model,     METH_VARARGS | METH_KEYWORDS,    model_docstring },
	{ "lingeling_nof_vars",  (PyCFunction) py_lingeling_nof_vars,  METH_VARARGS | METH_KEYWORDS,    nvars_docstring },
	{ "lingeling_nof_cls",   (PyCFunction) py_lingeling_nof_cls,   METH_VARARGS | METH_KEYWORDS,     ncls_docstring },
	{ "lingeling_del",       (PyCFunction) py_lingeling_del,       METH_VARARGS | METH_KEYWORDS,      del_docstring },
	{ "lingeling_acc_stats", (PyCFunction) py_lingeling_acc_stats, METH_VARARGS | METH_KEYWORDS, acc_stat_docstring },
#endif
#ifdef WITH_MAPLECHRONO
	{ "maplechrono_new",       (PyCFunction) py_maplechrono_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "maplechrono_add_cl",    (PyCFunction) py_maplechrono_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "maplechrono_solve",     (PyCFunction) py_maplechrono_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "maplechrono_solve_lim", (PyCFunction) py_maplechrono_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "maplechrono_propagate", (PyCFunction) py_maplechrono_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "maplechrono_setphases", (PyCFunction) py_maplechrono_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "maplechrono_cbudget",   (PyCFunction) py_maplechrono_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "maplechrono_pbudget",   (PyCFunction) py_maplechrono_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "maplechrono_interrupt", (PyCFunction) py_maplechrono_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "maplechrono_clearint",  (PyCFunction) py_maplechrono_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "maplechrono_tracepr",   (PyCFunction) py_maplechrono_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "maplechrono_core",      (PyCFunction) py_maplechrono_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "maplechrono_model",     (PyCFunction) py_maplechrono_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "maplechrono_nof_vars",  (PyCFunction) py_maplechrono_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "maplechrono_nof_cls",   (PyCFunction) py_maplechrono_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "maplechrono_del",       (PyCFunction) py_maplechrono_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "maplechrono_acc_stats", (PyCFunction) py_maplechrono_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_MAPLECM
	{ "maplecm_new",       (PyCFunction) py_maplecm_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "maplecm_add_cl",    (PyCFunction) py_maplecm_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "maplecm_solve",     (PyCFunction) py_maplecm_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "maplecm_solve_lim", (PyCFunction) py_maplecm_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "maplecm_propagate", (PyCFunction) py_maplecm_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "maplecm_setphases", (PyCFunction) py_maplecm_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "maplecm_cbudget",   (PyCFunction) py_maplecm_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "maplecm_pbudget",   (PyCFunction) py_maplecm_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "maplecm_interrupt", (PyCFunction) py_maplecm_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "maplecm_clearint",  (PyCFunction) py_maplecm_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "maplecm_tracepr",   (PyCFunction) py_maplecm_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "maplecm_core",      (PyCFunction) py_maplecm_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "maplecm_model",     (PyCFunction) py_maplecm_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "maplecm_nof_vars",  (PyCFunction) py_maplecm_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "maplecm_nof_cls",   (PyCFunction) py_maplecm_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "maplecm_del",       (PyCFunction) py_maplecm_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "maplecm_acc_stats", (PyCFunction) py_maplecm_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_MAPLESAT
	{ "maplesat_new",       (PyCFunction) py_maplesat_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "maplesat_add_cl",    (PyCFunction) py_maplesat_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "maplesat_solve",     (PyCFunction) py_maplesat_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "maplesat_solve_lim", (PyCFunction) py_maplesat_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "maplesat_propagate", (PyCFunction) py_maplesat_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "maplesat_setphases", (PyCFunction) py_maplesat_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "maplesat_cbudget",   (PyCFunction) py_maplesat_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "maplesat_pbudget",   (PyCFunction) py_maplesat_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "maplesat_interrupt", (PyCFunction) py_maplesat_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "maplesat_clearint",  (PyCFunction) py_maplesat_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "maplesat_tracepr",   (PyCFunction) py_maplesat_tracepr,   METH_VARARGS | METH_KEYWORDS,   tracepr_docstring },
	{ "maplesat_core",      (PyCFunction) py_maplesat_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "maplesat_model",     (PyCFunction) py_maplesat_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "maplesat_nof_vars",  (PyCFunction) py_maplesat_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "maplesat_nof_cls",   (PyCFunction) py_maplesat_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "maplesat_del",       (PyCFunction) py_maplesat_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "maplesat_acc_stats", (PyCFunction) py_maplesat_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_MERGESAT3
	{ "mergesat3_new",       (PyCFunction) py_mergesat3_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "mergesat3_add_cl",    (PyCFunction) py_mergesat3_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "mergesat3_solve",     (PyCFunction) py_mergesat3_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "mergesat3_solve_lim", (PyCFunction) py_mergesat3_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "mergesat3_propagate", (PyCFunction) py_mergesat3_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "mergesat3_setphases", (PyCFunction) py_mergesat3_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "mergesat3_cbudget",   (PyCFunction) py_mergesat3_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "mergesat3_pbudget",   (PyCFunction) py_mergesat3_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "mergesat3_interrupt", (PyCFunction) py_mergesat3_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "mergesat3_clearint",  (PyCFunction) py_mergesat3_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "mergesat3_core",      (PyCFunction) py_mergesat3_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "mergesat3_model",     (PyCFunction) py_mergesat3_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "mergesat3_nof_vars",  (PyCFunction) py_mergesat3_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "mergesat3_nof_cls",   (PyCFunction) py_mergesat3_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "mergesat3_del",       (PyCFunction) py_mergesat3_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "mergesat3_acc_stats", (PyCFunction) py_mergesat3_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_MINICARD
	{ "minicard_new",       (PyCFunction) py_minicard_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "minicard_add_cl",    (PyCFunction) py_minicard_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "minicard_solve",     (PyCFunction) py_minicard_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "minicard_solve_lim", (PyCFunction) py_minicard_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "minicard_propagate", (PyCFunction) py_minicard_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "minicard_setphases", (PyCFunction) py_minicard_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "minicard_cbudget",   (PyCFunction) py_minicard_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "minicard_pbudget",   (PyCFunction) py_minicard_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "minicard_interrupt", (PyCFunction) py_minicard_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "minicard_clearint",  (PyCFunction) py_minicard_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "minicard_core",      (PyCFunction) py_minicard_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "minicard_model",     (PyCFunction) py_minicard_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "minicard_nof_vars",  (PyCFunction) py_minicard_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "minicard_nof_cls",   (PyCFunction) py_minicard_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "minicard_del",       (PyCFunction) py_minicard_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "minicard_add_am",    (PyCFunction) py_minicard_add_am,    METH_VARARGS | METH_KEYWORDS,     addam_docstring },
	{ "minicard_acc_stats", (PyCFunction) py_minicard_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_MINISAT22
	{ "minisat22_new",       (PyCFunction) py_minisat22_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "minisat22_add_cl",    (PyCFunction) py_minisat22_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "minisat22_solve",     (PyCFunction) py_minisat22_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "minisat22_solve_lim", (PyCFunction) py_minisat22_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "minisat22_propagate", (PyCFunction) py_minisat22_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "minisat22_setphases", (PyCFunction) py_minisat22_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "minisat22_cbudget",   (PyCFunction) py_minisat22_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "minisat22_pbudget",   (PyCFunction) py_minisat22_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "minisat22_interrupt", (PyCFunction) py_minisat22_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "minisat22_clearint",  (PyCFunction) py_minisat22_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "minisat22_core",      (PyCFunction) py_minisat22_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "minisat22_model",     (PyCFunction) py_minisat22_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "minisat22_nof_vars",  (PyCFunction) py_minisat22_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "minisat22_nof_cls",   (PyCFunction) py_minisat22_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "minisat22_del",       (PyCFunction) py_minisat22_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "minisat22_acc_stats", (PyCFunction) py_minisat22_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
#ifdef WITH_MINISATGH
	{ "minisatgh_new",       (PyCFunction) py_minisatgh_new,       METH_VARARGS | METH_KEYWORDS,       new_docstring },
	{ "minisatgh_add_cl",    (PyCFunction) py_minisatgh_add_cl,    METH_VARARGS | METH_KEYWORDS,     addcl_docstring },
	{ "minisatgh_solve",     (PyCFunction) py_minisatgh_solve,     METH_VARARGS | METH_KEYWORDS,     solve_docstring },
	{ "minisatgh_solve_lim", (PyCFunction) py_minisatgh_solve_lim, METH_VARARGS | METH_KEYWORDS,       lim_docstring },
	{ "minisatgh_propagate", (PyCFunction) py_minisatgh_propagate, METH_VARARGS | METH_KEYWORDS,      prop_docstring },
	{ "minisatgh_setphases", (PyCFunction) py_minisatgh_setphases, METH_VARARGS | METH_KEYWORDS,    phases_docstring },
	{ "minisatgh_cbudget",   (PyCFunction) py_minisatgh_cbudget,   METH_VARARGS | METH_KEYWORDS,   cbudget_docstring },
	{ "minisatgh_pbudget",   (PyCFunction) py_minisatgh_pbudget,   METH_VARARGS | METH_KEYWORDS,   pbudget_docstring },
	{ "minisatgh_interrupt", (PyCFunction) py_minisatgh_interrupt, METH_VARARGS | METH_KEYWORDS, interrupt_docstring },
	{ "minisatgh_clearint",  (PyCFunction) py_minisatgh_clearint,  METH_VARARGS | METH_KEYWORDS,  clearint_docstring },
	{ "minisatgh_core",      (PyCFunction) py_minisatgh_core,      METH_VARARGS | METH_KEYWORDS,      core_docstring },
	{ "minisatgh_model",     (PyCFunction) py_minisatgh_model,     METH_VARARGS | METH_KEYWORDS,     model_docstring },
	{ "minisatgh_nof_vars",  (PyCFunction) py_minisatgh_nof_vars,  METH_VARARGS | METH_KEYWORDS,     nvars_docstring },
	{ "minisatgh_nof_cls",   (PyCFunction) py_minisatgh_nof_cls,   METH_VARARGS | METH_KEYWORDS,      ncls_docstring },
	{ "minisatgh_del",       (PyCFunction) py_minisatgh_del,       METH_VARARGS | METH_KEYWORDS,       del_docstring },
	{ "minisatgh_acc_stats", (PyCFunction) py_minisatgh_acc_stats, METH_VARARGS | METH_KEYWORDS,  acc_stat_docstring },
#endif
    { NULL, NULL, 0,   NULL }
};

extern "C" {

// signal handler for SIGINT
//=============================================================================
static void sigint_handler(int signum)
{
	longjmp(env, -1);
}

#if PY_MAJOR_VERSION >= 3
// PyInt_asLong()
//=============================================================================
static int pyint_to_cint(PyObject *i_obj)
{
	return PyLong_AsLong(i_obj);
}

// PyInt_fromLong()
//=============================================================================
static PyObject *pyint_from_cint(int i)
{
	return PyLong_FromLong(i);
}

// PyInt_Check()
//=============================================================================
static int pyint_check(PyObject *i_obj)
{
	return PyLong_Check(i_obj);
}

// PyCapsule_New()
//=============================================================================
static PyObject *void_to_pyobj(void *ptr)
{
	return PyCapsule_New(ptr, NULL, NULL);
}

// PyCapsule_GetPointer()
//=============================================================================
static void *pyobj_to_void(PyObject *obj)
{
	return PyCapsule_GetPointer(obj, NULL);
}

// module initialization
//=============================================================================
static struct PyModuleDef module_def = {
	PyModuleDef_HEAD_INIT,
	"pysolvers",       /* m_name */
	module_docstring,  /* m_doc */
	-1,                /* m_size */
	module_methods,    /* m_methods */
	NULL,              /* m_reload */
	NULL,              /* m_traverse */
	NULL,              /* m_clear */
	NULL,              /* m_free */
};

PyMODINIT_FUNC PyInit_pysolvers(void)
{
	PyObject *m = PyModule_Create(&module_def);

	if (m == NULL)
		return NULL;

	SATError = PyErr_NewException((char *)"pysolvers.error", NULL, NULL);
	Py_INCREF(SATError);

	if (PyModule_AddObject(m, "error", SATError) < 0) {
		Py_DECREF(SATError);
		return NULL;
	}

	return m;
}
#else
// PyInt_asLong()
//=============================================================================
static int pyint_to_cint(PyObject *i_obj)
{
	return PyInt_AsLong(i_obj);
}

// PyInt_fromLong()
//=============================================================================
static PyObject *pyint_from_cint(int i)
{
	return PyInt_FromLong(i);
}

// PyInt_Check()
//=============================================================================
static int pyint_check(PyObject *i_obj)
{
	return PyInt_Check(i_obj);
}

// PyCObject_FromVoidPtr()
//=============================================================================
static PyObject *void_to_pyobj(void *ptr)
{
	return PyCObject_FromVoidPtr(ptr, NULL);
}

// PyCObject_AsVoidPtr()
//=============================================================================
static void *pyobj_to_void(PyObject *obj)
{
	return PyCObject_AsVoidPtr(obj);
}

// module initialization
//=============================================================================
PyMODINIT_FUNC initpysolvers(void)
{
	PyObject *m = Py_InitModule3("pysolvers", module_methods,
			module_docstring);

	if (m == NULL)
		return;

	SATError = PyErr_NewException((char *)"pysolvers.error", NULL, NULL);
	Py_INCREF(SATError);
	PyModule_AddObject(m, "error", SATError);
}
#endif

// auxiliary function for translating an iterable to a vector<int>
//=============================================================================
static bool pyiter_to_vector(PyObject *obj, vector<int>& vect, int& max_var)
{
	PyObject *i_obj = PyObject_GetIter(obj);

	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		vect.push_back(l);

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

// API for CaDiCaL
//=============================================================================
#ifdef WITH_CADICAL
static PyObject *py_cadical_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	CaDiCaL::Solver *s = new CaDiCaL::Solver;

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

//
//=============================================================================
static PyObject *py_cadical_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)pyobj_to_void(s_obj);

	// clause iterator
	PyObject *i_obj = PyObject_GetIter(c_obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Clause does not seem to be an iterable object.");
		return NULL;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return NULL;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return NULL;
		}

		s->add(l);
	}

	s->add(0);
	Py_DECREF(i_obj);

	PyObject *ret = PyBool_FromLong((long)true);
	return ret;
}

//
//=============================================================================
static PyObject *py_cadical_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->trace_proof(PyFile_AsFile(p_obj), "<py_fobj>");
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	FILE *cd_trace_fp = fdopen(fd, "w+");
	if (cd_trace_fp == NULL) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(cd_trace_fp);
	s->trace_proof(cd_trace_fp, "<py_fobj>");
	Py_INCREF(p_obj);
#endif

	s->set("binary", 0);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_cadical_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)pyobj_to_void(s_obj);

	// assumptions iterator
	PyObject *i_obj = PyObject_GetIter(a_obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return NULL;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return NULL;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return NULL;
		}

		s->assume(l);
	}

	Py_DECREF(i_obj);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve() == 10 ? true : false;

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_cadical_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &a_obj))
		return NULL;

	// get pointer to solver
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)pyobj_to_void(s_obj);

	int size = (int)PyList_Size(a_obj);

	vector<int> c;
	for (int i = 0; i < size; ++i) {
		PyObject *l_obj = PyList_GetItem(a_obj, i);
		int l = pyint_to_cint(l_obj);

		if (s->failed(l))
			c.push_back(l);
	}

	PyObject *core = PyList_New(c.size());
	for (size_t i = 0; i < c.size(); ++i) {
		PyObject *lit = pyint_from_cint(c[i]);
		PyList_SetItem(core, i, lit);
	}

	if (c.size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_cadical_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)pyobj_to_void(s_obj);

	int maxvar = s->vars();
	if (maxvar) {
		PyObject *model = PyList_New(maxvar);
		for (int i = 1; i <= maxvar; ++i) {
			int l = s->val(i) > 0 ? i : -i;

			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_cadical_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->vars();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_cadical_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->irredundant() + s->redundant();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_cadical_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (p_obj != Py_None)
		PyFile_DecUseCount((PyFileObject *)p_obj);
#else
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (p_obj != Py_None)
		Py_DECREF(p_obj);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_cadical_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	CaDiCaL::Solver *s = (CaDiCaL::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->restarts(),
		"conflicts", s->conflicts(),
		"decisions", s->decisions(),
		"propagations", s->propagations()
	);
}
#endif  // WITH_CADICAL

// API for Gluecard 3.0
//=============================================================================
#ifdef WITH_GLUECARD30
static PyObject *py_gluecard3_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Gluecard30::Solver *s = new Gluecard30::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void gluecard3_declare_vars(Gluecard30::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool gluecard3_iterate(
	PyObject *obj,
	Gluecard30::vec<Gluecard30::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Gluecard30::mkLit(l, false) : Gluecard30::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_gluecard3_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);
	Gluecard30::vec<Gluecard30::Lit> cl;
	int max_var = -1;

	if (gluecard3_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard3_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard3_add_am(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;
	int64_t rhs;

	if (!PyArg_ParseTuple(args, "OOl", &s_obj, &c_obj, &rhs))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);
	Gluecard30::vec<Gluecard30::Lit> cl;
	int max_var = -1;

	if (gluecard3_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard3_declare_vars(s, max_var);

	bool res = s->addAtMost(cl, rhs);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard3_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);
	Gluecard30::vec<Gluecard30::Lit> a;
	int max_var = -1;

	if (gluecard3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard3_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard3_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);
	Gluecard30::vec<Gluecard30::Lit> a;
	int max_var = -1;

	if (gluecard3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard3_declare_vars(s, max_var);

	Gluecard30::lbool res = Gluecard30::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Gluecard30::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Gluecard30::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_gluecard3_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);
	Gluecard30::vec<Gluecard30::Lit> a;
	int max_var = -1;

	if (gluecard3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard3_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Gluecard30::vec<Gluecard30::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Gluecard30::var(p[i]) * (Gluecard30::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard3_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard3_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_setincr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	s->setIncrementalMode();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Gluecard30::Solver *s = (Gluecard30::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->certifiedOutput = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	Gluecard30::Solver *s = (Gluecard30::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->certifiedOutput = fdopen(fd, "w+");
	if (s->certifiedOutput == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->certifiedOutput);
	Py_INCREF(p_obj);
#endif

	s->certifiedUNSAT  = true;
	s->certifiedPyFile = (void *)p_obj;

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	Gluecard30::vec<Gluecard30::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Gluecard30::var((*c)[i]) * (Gluecard30::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Gluecard30::vec<Gluecard30::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Gluecard30::lbool True = Gluecard30::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard3_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard30::Solver *s = (Gluecard30::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard3_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Gluecard30::Solver *s = (Gluecard30::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->certifiedUNSAT == true)
		PyFile_DecUseCount((PyFileObject *)(s->certifiedPyFile));
#else
	Gluecard30::Solver *s = (Gluecard30::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->certifiedUNSAT == true)
		Py_DECREF((PyObject *)s->certifiedPyFile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard3_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Gluecard30::Solver *s = (Gluecard30::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Gluecard30::Solver *s = (Gluecard30::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_GLUECARD30

// API for Gluecard 3.0
//=============================================================================
#ifdef WITH_GLUECARD41
static PyObject *py_gluecard41_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Gluecard41::Solver *s = new Gluecard41::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void gluecard41_declare_vars(Gluecard41::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool gluecard41_iterate(
	PyObject *obj,
	Gluecard41::vec<Gluecard41::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Gluecard41::mkLit(l, false) : Gluecard41::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_gluecard41_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);
	Gluecard41::vec<Gluecard41::Lit> cl;
	int max_var = -1;

	if (gluecard41_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard41_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard41_add_am(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;
	int64_t rhs;

	if (!PyArg_ParseTuple(args, "OOl", &s_obj, &c_obj, &rhs))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);
	Gluecard41::vec<Gluecard41::Lit> cl;
	int max_var = -1;

	if (gluecard41_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard41_declare_vars(s, max_var);

	bool res = s->addAtMost(cl, rhs);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard41_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);
	Gluecard41::vec<Gluecard41::Lit> a;
	int max_var = -1;

	if (gluecard41_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard41_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard41_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);
	Gluecard41::vec<Gluecard41::Lit> a;
	int max_var = -1;

	if (gluecard41_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard41_declare_vars(s, max_var);

	Gluecard41::lbool res = Gluecard41::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Gluecard41::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Gluecard41::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_gluecard41_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);
	Gluecard41::vec<Gluecard41::Lit> a;
	int max_var = -1;

	if (gluecard41_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard41_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Gluecard41::vec<Gluecard41::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Gluecard41::var(p[i]) * (Gluecard41::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard41_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		gluecard41_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_setincr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	s->setIncrementalMode();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Gluecard41::Solver *s = (Gluecard41::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->certifiedOutput = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	Gluecard41::Solver *s = (Gluecard41::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->certifiedOutput = fdopen(fd, "w+");
	if (s->certifiedOutput == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->certifiedOutput);
	Py_INCREF(p_obj);
#endif

	s->certifiedUNSAT  = true;
	s->certifiedPyFile = (void *)p_obj;

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	Gluecard41::vec<Gluecard41::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Gluecard41::var((*c)[i]) * (Gluecard41::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Gluecard41::vec<Gluecard41::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Gluecard41::lbool True = Gluecard41::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard41_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Gluecard41::Solver *s = (Gluecard41::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_gluecard41_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Gluecard41::Solver *s = (Gluecard41::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->certifiedUNSAT == true)
		PyFile_DecUseCount((PyFileObject *)(s->certifiedPyFile));
#else
	Gluecard41::Solver *s = (Gluecard41::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->certifiedUNSAT == true)
		Py_DECREF((PyObject *)s->certifiedPyFile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_gluecard41_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Gluecard41::Solver *s = (Gluecard41::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Gluecard41::Solver *s = (Gluecard41::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_GLUECARD41

// API for Glucose 3.0
//=============================================================================
#ifdef WITH_GLUCOSE30
static PyObject *py_glucose3_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Glucose30::Solver *s = new Glucose30::Solver();

    PyObject *pyX = NULL;

    double *seed = nullptr;

    static char *kwlist[] = {"random_seed", NULL};

    PyObject *empty = PyTuple_New(0);

    if (PyArg_ParseTupleAndKeywords(empty, kwargs, "|O:value", const_cast<char**>(kwlist), &pyX)) {
        if (pyX != NULL) {
            double x = (double) PyLong_AsDouble(pyX);
            s->random_seed = x;
        }
    }

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void glucose3_declare_vars(Glucose30::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool glucose3_iterate(
	PyObject *obj,
	Glucose30::vec<Glucose30::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Glucose30::mkLit(l, false) : Glucose30::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_glucose3_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);
	Glucose30::vec<Glucose30::Lit> cl;
	int max_var = -1;

	if (glucose3_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose3_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose3_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);
	Glucose30::vec<Glucose30::Lit> a;
	int max_var = -1;

	if (glucose3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose3_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose3_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);
	Glucose30::vec<Glucose30::Lit> a;
	int max_var = -1;

	if (glucose3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose3_declare_vars(s, max_var);

	Glucose30::lbool res = Glucose30::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Glucose30::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Glucose30::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_glucose3_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);
	Glucose30::vec<Glucose30::Lit> a;
	int max_var = -1;

	if (glucose3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose3_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Glucose30::vec<Glucose30::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Glucose30::var(p[i]) * (Glucose30::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_glucose3_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose3_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_setincr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	s->setIncrementalMode();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Glucose30::Solver *s = (Glucose30::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->certifiedOutput = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	Glucose30::Solver *s = (Glucose30::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->certifiedOutput = fdopen(fd, "w+");
	if (s->certifiedOutput == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->certifiedOutput);
	Py_INCREF(p_obj);
#endif

	s->certifiedUNSAT  = true;
	s->certifiedPyFile = (void *)p_obj;

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	Glucose30::vec<Glucose30::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Glucose30::var((*c)[i]) * (Glucose30::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Glucose30::vec<Glucose30::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Glucose30::lbool True = Glucose30::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose3_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose30::Solver *s = (Glucose30::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose3_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Glucose30::Solver *s = (Glucose30::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->certifiedUNSAT == true)
		PyFile_DecUseCount((PyFileObject *)(s->certifiedPyFile));
#else
	Glucose30::Solver *s = (Glucose30::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->certifiedUNSAT == true)
		Py_DECREF((PyObject *)s->certifiedPyFile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose3_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Glucose30::Solver *s = (Glucose30::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Glucose30::Solver *s = (Glucose30::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_GLUCOSE30

// API for Glucose 4.1
//=============================================================================
#ifdef WITH_GLUCOSE41
static PyObject *py_glucose41_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Glucose41::Solver *s = new Glucose41::Solver();

    const double *seed;

    static char *kwlist[] = {"random_seed", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwargs, "d", kwlist,
                                     &seed)) {
        printf("-- Seed is %d\n", *seed);
        s->random_seed = *seed;
    }

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void glucose41_declare_vars(Glucose41::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool glucose41_iterate(
	PyObject *obj,
	Glucose41::vec<Glucose41::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Glucose41::mkLit(l, false) : Glucose41::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_glucose41_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);
	Glucose41::vec<Glucose41::Lit> cl;
	int max_var = -1;

	if (glucose41_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose41_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose41_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);
	Glucose41::vec<Glucose41::Lit> a;
	int max_var = -1;

	if (glucose41_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose41_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose41_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);
	Glucose41::vec<Glucose41::Lit> a;
	int max_var = -1;

	if (glucose41_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose41_declare_vars(s, max_var);

	Glucose41::lbool res = Glucose41::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Glucose41::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Glucose41::toInt(res)));

	Py_RETURN_NONE; // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_glucose41_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);
	Glucose41::vec<Glucose41::Lit> a;
	int max_var = -1;

	if (glucose41_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose41_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Glucose41::vec<Glucose41::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Glucose41::var(p[i]) * (Glucose41::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_glucose41_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		glucose41_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_setincr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	s->setIncrementalMode();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Glucose41::Solver *s = (Glucose41::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->certifiedOutput = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	Glucose41::Solver *s = (Glucose41::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->certifiedOutput = fdopen(fd, "w+");
	if (s->certifiedOutput == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->certifiedOutput);
	Py_INCREF(p_obj);
#endif

	s->certifiedUNSAT  = true;
	s->certifiedPyFile = (void *)p_obj;

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	Glucose41::vec<Glucose41::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Glucose41::var((*c)[i]) * (Glucose41::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Glucose41::vec<Glucose41::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Glucose41::lbool True = Glucose41::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose41_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Glucose41::Solver *s = (Glucose41::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_glucose41_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Glucose41::Solver *s = (Glucose41::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->certifiedUNSAT == true)
		PyFile_DecUseCount((PyFileObject *)(s->certifiedPyFile));
#else
	Glucose41::Solver *s = (Glucose41::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->certifiedUNSAT == true)
		Py_DECREF((PyObject *)s->certifiedPyFile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_glucose41_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Glucose41::Solver *s = (Glucose41::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Glucose41::Solver *s = (Glucose41::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_GLUCOSE41

// API for Lingeling
//=============================================================================
#ifdef WITH_LINGELING
static PyObject *py_lingeling_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	LGL *s = lglinit();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	lglsetopt(s, "simplify", 0);

	return void_to_pyobj((void *)s);
}

//
//=============================================================================
static PyObject *py_lingeling_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	// clause iterator
	PyObject *i_obj = PyObject_GetIter(c_obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Clause does not seem to be an iterable object.");
		return NULL;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return NULL;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return NULL;
		}

		lgladd(s, l);
		lglfreeze(s, abs(l));
	}

	lgladd(s, 0);
	Py_DECREF(i_obj);

	PyObject *ret = PyBool_FromLong((long)true);
	return ret;
}

//
//=============================================================================
static PyObject *py_lingeling_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	LGL *s = (LGL *)PyCObject_AsVoidPtr(s_obj);

	lglsetrace(s, PyFile_AsFile(p_obj));
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	LGL *s = (LGL *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	FILE *lgl_trace_fp = fdopen(fd, "w+");
	if (lgl_trace_fp == NULL) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(lgl_trace_fp);
	lglsetrace(s, lgl_trace_fp);
	Py_INCREF(p_obj);
#endif

	lglsetopt (s, "druplig", 1);
	lglsetopt (s, "drupligtrace", 2);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_lingeling_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	// assumptions iterator
	PyObject *i_obj = PyObject_GetIter(a_obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return NULL;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return NULL;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return NULL;
		}

		lglassume(s, l);
	}

	Py_DECREF(i_obj);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = lglsat(s) == 10 ? true : false;

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_lingeling_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	// phases iterator
	PyObject *i_obj = PyObject_GetIter(p_obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return NULL;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return NULL;
		}

		int lit = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (lit == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return NULL;
		}

		lglsetphase(s, lit);
	}

	Py_DECREF(i_obj);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_lingeling_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &a_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	int size = (int)PyList_Size(a_obj);

	vector<int> c;
	for (int i = 0; i < size; ++i) {
		PyObject *l_obj = PyList_GetItem(a_obj, i);
		int l = pyint_to_cint(l_obj);

		if (lglfailed(s, l))
			c.push_back(l);
	}

	PyObject *core = PyList_New(c.size());
	for (size_t i = 0; i < c.size(); ++i) {
		PyObject *lit = pyint_from_cint(c[i]);
		PyList_SetItem(core, i, lit);
	}

	if (c.size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_lingeling_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	int maxvar = lglmaxvar(s);
	if (maxvar) {
		PyObject *model = PyList_New(maxvar);
		for (int i = 1; i <= maxvar; ++i) {
			int l = lglderef(s, i) > 0 ? i : -i;

			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_lingeling_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	int nof_vars = lglmaxvar(s);

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_lingeling_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	int nof_cls = lglnclauses(s);

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_lingeling_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	LGL *s = (LGL *)PyCObject_AsVoidPtr(s_obj);

	if (p_obj != Py_None)
		PyFile_DecUseCount((PyFileObject *)p_obj);
#else
	LGL *s = (LGL *)PyCapsule_GetPointer(s_obj, NULL);

	if (p_obj != Py_None)
		Py_DECREF(p_obj);
#endif

	lglrelease(s);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_lingeling_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	LGL *s = (LGL *)pyobj_to_void(s_obj);

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", lglgetrests(s),
		"conflicts", lglgetconfs(s),
		"decisions", lglgetdecs(s),
		"propagations", lglgetprops(s)
	);
}
#endif  // WITH_LINGELING

// API for MapleChrono
//=============================================================================
#ifdef WITH_MAPLECHRONO
static PyObject *py_maplechrono_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	MapleChrono::Solver *s = new MapleChrono::Solver();

    const double *seed;

    static char *kwlist[] = {"random_seed", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwargs, "d", kwlist,
                                     &seed)) {
        printf("-- Seed is %d\n", *seed);
        s->random_seed = *seed;
    }


	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void maplechrono_declare_vars(
	MapleChrono::Solver *s,
	const int max_id
)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool maplechrono_iterate(
	PyObject *obj,
	MapleChrono::vec<MapleChrono::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? MapleChrono::mkLit(l, false) : MapleChrono::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_maplechrono_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);
	MapleChrono::vec<MapleChrono::Lit> cl;
	int max_var = -1;

	if (maplechrono_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplechrono_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplechrono_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);
	MapleChrono::vec<MapleChrono::Lit> a;
	int max_var = -1;

	if (maplechrono_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplechrono_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplechrono_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);
	MapleChrono::vec<MapleChrono::Lit> a;
	int max_var = -1;

	if (maplechrono_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplechrono_declare_vars(s, max_var);

	MapleChrono::lbool res = MapleChrono::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != MapleChrono::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(MapleChrono::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_maplechrono_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);
	MapleChrono::vec<MapleChrono::Lit> a;
	int max_var = -1;

	if (maplechrono_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplechrono_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	MapleChrono::vec<MapleChrono::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = MapleChrono::var(p[i]) * (MapleChrono::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_maplechrono_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplechrono_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MapleChrono::Solver *s = (MapleChrono::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->drup_file = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	MapleChrono::Solver *s = (MapleChrono::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->drup_file = fdopen(fd, "w+");
	if (s->drup_file == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->drup_file);
	Py_INCREF(p_obj);
#endif

	s->drup_pyfile = (void *)p_obj;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	MapleChrono::vec<MapleChrono::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = MapleChrono::var((*c)[i]) * (MapleChrono::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	MapleChrono::vec<MapleChrono::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		MapleChrono::lbool True = MapleChrono::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplechrono_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleChrono::Solver *s = (MapleChrono::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplechrono_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MapleChrono::Solver *s = (MapleChrono::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->drup_file)
		PyFile_DecUseCount((PyFileObject *)(s->drup_pyfile));
#else
	MapleChrono::Solver *s = (MapleChrono::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->drup_file)
		Py_DECREF((PyObject *)s->drup_pyfile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplechrono_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MapleChrono::Solver *s = (MapleChrono::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	MapleChrono::Solver *s = (MapleChrono::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MAPLECHRONO

// API for Maplesat
//=============================================================================
#ifdef WITH_MAPLESAT
static PyObject *py_maplesat_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Maplesat::Solver *s = new Maplesat::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void maplesat_declare_vars(
	Maplesat::Solver *s,
	const int max_id
)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool maplesat_iterate(
	PyObject *obj,
	Maplesat::vec<Maplesat::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Maplesat::mkLit(l, false) : Maplesat::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_maplesat_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);
	Maplesat::vec<Maplesat::Lit> cl;
	int max_var = -1;

	if (maplesat_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplesat_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplesat_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);
	Maplesat::vec<Maplesat::Lit> a;
	int max_var = -1;

	if (maplesat_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplesat_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplesat_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);
	Maplesat::vec<Maplesat::Lit> a;
	int max_var = -1;

	if (maplesat_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplesat_declare_vars(s, max_var);

	Maplesat::lbool res = Maplesat::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Maplesat::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Maplesat::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_maplesat_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);
	Maplesat::vec<Maplesat::Lit> a;
	int max_var = -1;

	if (maplesat_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplesat_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Maplesat::vec<Maplesat::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Maplesat::var(p[i]) * (Maplesat::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_maplesat_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplesat_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Maplesat::Solver *s = (Maplesat::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->drup_file = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	Maplesat::Solver *s = (Maplesat::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->drup_file = fdopen(fd, "w+");
	if (s->drup_file == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->drup_file);
	Py_INCREF(p_obj);
#endif

	s->drup_pyfile = (void *)p_obj;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	Maplesat::vec<Maplesat::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Maplesat::var((*c)[i]) * (Maplesat::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Maplesat::vec<Maplesat::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Maplesat::lbool True = Maplesat::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplesat_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Maplesat::Solver *s = (Maplesat::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplesat_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Maplesat::Solver *s = (Maplesat::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->drup_file)
		PyFile_DecUseCount((PyFileObject *)(s->drup_pyfile));
#else
	Maplesat::Solver *s = (Maplesat::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->drup_file)
		Py_DECREF((PyObject *)s->drup_pyfile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplesat_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Maplesat::Solver *s = (Maplesat::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Maplesat::Solver *s = (Maplesat::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MAPLESAT

// API for MapleCM
//=============================================================================
#ifdef WITH_MAPLECM
static PyObject *py_maplecm_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	MapleCM::Solver *s = new MapleCM::Solver();

    const double *seed;

    static char *kwlist[] = {"random_seed", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwargs, "d", kwlist,
                                     &seed)) {
        printf("-- Seed is %d\n", *seed);
        s->random_seed = *seed;
    }

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void maplecm_declare_vars(
	MapleCM::Solver *s,
	const int max_id
)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool maplecm_iterate(
	PyObject *obj,
	MapleCM::vec<MapleCM::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? MapleCM::mkLit(l, false) : MapleCM::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_maplecm_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);
	MapleCM::vec<MapleCM::Lit> cl;
	int max_var = -1;

	if (maplecm_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplecm_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplecm_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);
	MapleCM::vec<MapleCM::Lit> a;
	int max_var = -1;

	if (maplecm_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplecm_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplecm_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);
	MapleCM::vec<MapleCM::Lit> a;
	int max_var = -1;

	if (maplecm_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplecm_declare_vars(s, max_var);

	MapleCM::lbool res = MapleCM::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != MapleCM::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(MapleCM::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_maplecm_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);
	MapleCM::vec<MapleCM::Lit> a;
	int max_var = -1;

	if (maplecm_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplecm_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	MapleCM::vec<MapleCM::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = MapleCM::var(p[i]) * (MapleCM::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_maplecm_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		maplecm_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_tracepr(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MapleCM::Solver *s = (MapleCM::Solver *)PyCObject_AsVoidPtr(s_obj);

	s->drup_file = PyFile_AsFile(p_obj);
	PyFile_IncUseCount((PyFileObject *)p_obj);
#else
	MapleCM::Solver *s = (MapleCM::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	int fd = PyObject_AsFileDescriptor(p_obj);
	if (fd == -1) {
		PyErr_SetString(SATError, "Cannot create proof file descriptor!");
		return NULL;
	}

	s->drup_file = fdopen(fd, "w+");
	if (s->drup_file == 0) {
		PyErr_SetString(SATError, "Cannot create proof file pointer!");
		return NULL;
	}

	setlinebuf(s->drup_file);
	Py_INCREF(p_obj);
#endif

	s->drup_pyfile = (void *)p_obj;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	MapleCM::vec<MapleCM::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = MapleCM::var((*c)[i]) * (MapleCM::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	MapleCM::vec<MapleCM::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		MapleCM::lbool True = MapleCM::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplecm_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MapleCM::Solver *s = (MapleCM::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_maplecm_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MapleCM::Solver *s = (MapleCM::Solver *)PyCObject_AsVoidPtr(s_obj);

	if (s->drup_file)
		PyFile_DecUseCount((PyFileObject *)(s->drup_pyfile));
#else
	MapleCM::Solver *s = (MapleCM::Solver *)PyCapsule_GetPointer(s_obj, NULL);

	if (s->drup_file)
		Py_DECREF((PyObject *)s->drup_pyfile);
#endif

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_maplecm_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MapleCM::Solver *s = (MapleCM::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	MapleCM::Solver *s = (MapleCM::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MAPLECM

// API for MergeSat 3
//=============================================================================
#ifdef WITH_MERGESAT3
static PyObject *py_mergesat3_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	MergeSat3::Solver *s = new MergeSat3::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void mergesat3_declare_vars(MergeSat3::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool mergesat3_iterate(
	PyObject *obj,
	MergeSat3::vec<MergeSat3::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? MergeSat3::mkLit(l, false) : MergeSat3::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_mergesat3_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);
	MergeSat3::vec<MergeSat3::Lit> cl;
	int max_var = -1;

	if (mergesat3_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		mergesat3_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_mergesat3_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);
	MergeSat3::vec<MergeSat3::Lit> a;
	int max_var = -1;

	if (mergesat3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		mergesat3_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_mergesat3_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);
	MergeSat3::vec<MergeSat3::Lit> a;
	int max_var = -1;

	if (mergesat3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		mergesat3_declare_vars(s, max_var);

	MergeSat3::lbool res = MergeSat3::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != MergeSat3::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(MergeSat3::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_mergesat3_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);
	MergeSat3::vec<MergeSat3::Lit> a;
	int max_var = -1;

	if (mergesat3_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		mergesat3_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	MergeSat3::vec<MergeSat3::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = MergeSat3::var(p[i]) * (MergeSat3::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_mergesat3_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		mergesat3_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	MergeSat3::vec<MergeSat3::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = MergeSat3::var((*c)[i]) * (MergeSat3::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	MergeSat3::vec<MergeSat3::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		MergeSat3::lbool True = MergeSat3::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_mergesat3_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_mergesat3_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MergeSat3::Solver *s = (MergeSat3::Solver *)pyobj_to_void(s_obj);

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_mergesat3_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MergeSat3::Solver *s = (MergeSat3::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	MergeSat3::Solver *s = (MergeSat3::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MERGESAT3

// API for Minicard
//=============================================================================
#ifdef WITH_MINICARD
static PyObject *py_minicard_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Minicard::Solver *s = new Minicard::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void minicard_declare_vars(Minicard::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool minicard_iterate(
	PyObject *obj,
	Minicard::vec<Minicard::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Minicard::mkLit(l, false) : Minicard::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_minicard_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);
	Minicard::vec<Minicard::Lit> cl;
	int max_var = -1;

	if (minicard_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		minicard_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minicard_add_am(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;
	int64_t rhs;

	if (!PyArg_ParseTuple(args, "OOl", &s_obj, &c_obj, &rhs))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);
	Minicard::vec<Minicard::Lit> cl;
	int max_var = -1;

	if (minicard_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		minicard_declare_vars(s, max_var);

	bool res = s->addAtMost(cl, rhs);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minicard_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);
	Minicard::vec<Minicard::Lit> a;
	int max_var = -1;

	if (minicard_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minicard_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minicard_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);
	Minicard::vec<Minicard::Lit> a;
	int max_var = -1;

	if (minicard_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minicard_declare_vars(s, max_var);

	Minicard::lbool res = Minicard::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Minicard::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Minicard::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_minicard_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);
	Minicard::vec<Minicard::Lit> a;
	int max_var = -1;

	if (minicard_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minicard_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Minicard::vec<Minicard::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Minicard::var(p[i]) * (Minicard::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_minicard_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		minicard_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	Minicard::vec<Minicard::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Minicard::var((*c)[i]) * (Minicard::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Minicard::vec<Minicard::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Minicard::lbool True = Minicard::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_minicard_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_minicard_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minicard::Solver *s = (Minicard::Solver *)pyobj_to_void(s_obj);

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minicard_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Minicard::Solver *s = (Minicard::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Minicard::Solver *s = (Minicard::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MINICARD

// API for MiniSat 2.2
//=============================================================================
#ifdef WITH_MINISAT22
static PyObject *py_minisat22_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	Minisat22::Solver *s = new Minisat22::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void minisat22_declare_vars(Minisat22::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool minisat22_iterate(
	PyObject *obj,
	Minisat22::vec<Minisat22::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? Minisat22::mkLit(l, false) : Minisat22::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_minisat22_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);
	Minisat22::vec<Minisat22::Lit> cl;
	int max_var = -1;

	if (minisat22_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisat22_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisat22_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);
	Minisat22::vec<Minisat22::Lit> a;
	int max_var = -1;

	if (minisat22_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisat22_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisat22_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);
	Minisat22::vec<Minisat22::Lit> a;
	int max_var = -1;

	if (minisat22_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisat22_declare_vars(s, max_var);

	Minisat22::lbool res = Minisat22::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != Minisat22::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(Minisat22::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_minisat22_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);
	Minisat22::vec<Minisat22::Lit> a;
	int max_var = -1;

	if (minisat22_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisat22_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	Minisat22::vec<Minisat22::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = Minisat22::var(p[i]) * (Minisat22::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_minisat22_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisat22_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), p[i] < 0);

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	Minisat22::vec<Minisat22::Lit> *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = Minisat22::var((*c)[i]) * (Minisat22::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	Minisat22::vec<Minisat22::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		Minisat22::lbool True = Minisat22::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisat22_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisat22_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	Minisat22::Solver *s = (Minisat22::Solver *)pyobj_to_void(s_obj);

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisat22_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	Minisat22::Solver *s = (Minisat22::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	Minisat22::Solver *s = (Minisat22::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MINISAT22

// API for MiniSat from github
//=============================================================================
#ifdef WITH_MINISATGH
static PyObject *py_minisatgh_new(PyObject *self, PyObject *args, PyObject *kwargs)
{
	MinisatGH::Solver *s = new MinisatGH::Solver();

	if (s == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Cannot create a new solver.");
		return NULL;
	}

	return void_to_pyobj((void *)s);
}

// auxiliary function for declaring new variables
//=============================================================================
static inline void minisatgh_declare_vars(MinisatGH::Solver *s, const int max_id)
{
	while (s->nVars() < max_id + 1)
		s->newVar();
}

// translating an iterable to vec<Lit>
//=============================================================================
static inline bool minisatgh_iterate(
	PyObject *obj,
	MinisatGH::vec<MinisatGH::Lit>& v,
	int& max_var
)
{
	// iterator object
	PyObject *i_obj = PyObject_GetIter(obj);
	if (i_obj == NULL) {
		PyErr_SetString(PyExc_RuntimeError,
				"Object does not seem to be an iterable.");
		return false;
	}

	PyObject *l_obj;
	while ((l_obj = PyIter_Next(i_obj)) != NULL) {
		if (!pyint_check(l_obj)) {
			Py_DECREF(l_obj);
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_TypeError, "integer expected");
			return false;
		}

		int l = pyint_to_cint(l_obj);
		Py_DECREF(l_obj);

		if (l == 0) {
			Py_DECREF(i_obj);
			PyErr_SetString(PyExc_ValueError, "non-zero integer expected");
			return false;
		}

		v.push((l > 0) ? MinisatGH::mkLit(l, false) : MinisatGH::mkLit(-l, true));

		if (abs(l) > max_var)
			max_var = abs(l);
	}

	Py_DECREF(i_obj);
	return true;
}

//
//=============================================================================
static PyObject *py_minisatgh_add_cl(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *c_obj;

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &c_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);
	MinisatGH::vec<MinisatGH::Lit> cl;
	int max_var = -1;

	if (minisatgh_iterate(c_obj, cl, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisatgh_declare_vars(s, max_var);

	bool res = s->addClause(cl);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisatgh_solve(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOi", &s_obj, &a_obj, &main_thread))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);
	MinisatGH::vec<MinisatGH::Lit> a;
	int max_var = -1;

	if (minisatgh_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisatgh_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	bool res = s->solve(a);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *ret = PyBool_FromLong((long)res);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisatgh_solve_lim(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int main_thread;
	int expect_interrupt;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &main_thread,
				&expect_interrupt))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);
	MinisatGH::vec<MinisatGH::Lit> a;
	int max_var = -1;

	if (minisatgh_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisatgh_declare_vars(s, max_var);

	MinisatGH::lbool res = MinisatGH::lbool((uint8_t)2);  // l_Undef
	if (expect_interrupt == 0) {
		PyOS_sighandler_t sig_save;
		if (main_thread) {
			sig_save = PyOS_setsig(SIGINT, sigint_handler);

			if (setjmp(env) != 0) {
				PyErr_SetString(SATError, "Caught keyboard interrupt");
				return NULL;
			}
		}

		res = s->solveLimited(a);

		if (main_thread)
			PyOS_setsig(SIGINT, sig_save);
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		res = s->solveLimited(a);
		Py_END_ALLOW_THREADS
	}

	if (res != MinisatGH::lbool((uint8_t)2))  // l_Undef
		return PyBool_FromLong((long)!(MinisatGH::toInt(res)));

	Py_RETURN_NONE;  // return Python's None if l_Undef
}

//
//=============================================================================
static PyObject *py_minisatgh_propagate(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *a_obj;  // assumptions
	int save_phases;
	int main_thread;

	if (!PyArg_ParseTuple(args, "OOii", &s_obj, &a_obj, &save_phases,
				&main_thread))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);
	MinisatGH::vec<MinisatGH::Lit> a;
	int max_var = -1;

	if (minisatgh_iterate(a_obj, a, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisatgh_declare_vars(s, max_var);

	PyOS_sighandler_t sig_save;
	if (main_thread) {
		sig_save = PyOS_setsig(SIGINT, sigint_handler);

		if (setjmp(env) != 0) {
			PyErr_SetString(SATError, "Caught keyboard interrupt");
			return NULL;
		}
	}

	MinisatGH::vec<MinisatGH::Lit> p;
	bool res = s->prop_check(a, p, save_phases);

	if (main_thread)
		PyOS_setsig(SIGINT, sig_save);

	PyObject *propagated = PyList_New(p.size());
	for (int i = 0; i < p.size(); ++i) {
		int l = MinisatGH::var(p[i]) * (MinisatGH::sign(p[i]) ? -1 : 1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(propagated, i, lit);
	}

	PyObject *ret = Py_BuildValue("nO", (Py_ssize_t)res, propagated);
	Py_DECREF(propagated);

	return ret;
}

//
//=============================================================================
static PyObject *py_minisatgh_setphases(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	PyObject *p_obj;  // polarities given as a list of integer literals

	if (!PyArg_ParseTuple(args, "OO", &s_obj, &p_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);
	vector<int> p;
	int max_var = -1;

	if (pyiter_to_vector(p_obj, p, max_var) == false)
		return NULL;

	if (max_var > 0)
		minisatgh_declare_vars(s, max_var);

	for (size_t i = 0; i < p.size(); ++i)
		s->setPolarity(abs(p[i]), MinisatGH::lbool(p[i] < 0));

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_cbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setConfBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_pbudget(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;
	int64_t budget;

	if (!PyArg_ParseTuple(args, "Ol", &s_obj, &budget))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	if (budget != 0 && budget != -1)  // it is 0 by default
		s->setPropBudget(budget);
	else
		s->budgetOff();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_interrupt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	s->interrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_clearint(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	s->clearInterrupt();

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_core(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	MinisatGH::LSet *c = &(s->conflict);  // minisat's conflict

	PyObject *core = PyList_New(c->size());
	for (int i = 0; i < c->size(); ++i) {
		int l = MinisatGH::var((*c)[i]) * (MinisatGH::sign((*c)[i]) ? 1 : -1);
		PyObject *lit = pyint_from_cint(l);
		PyList_SetItem(core, i, lit);
	}

	if (c->size()) {
		PyObject *ret = Py_BuildValue("O", core);
		Py_DECREF(core);
		return ret;
	}

	Py_DECREF(core);
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_model(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	// minisat's model
	MinisatGH::vec<MinisatGH::lbool> *m = &(s->model);

	if (m->size()) {
		// l_True fails to work
		MinisatGH::lbool True = MinisatGH::lbool((uint8_t)0);

		PyObject *model = PyList_New(m->size() - 1);
		for (int i = 1; i < m->size(); ++i) {
			int l = i * ((*m)[i] == True ? 1 : -1);
			PyObject *lit = pyint_from_cint(l);
			PyList_SetItem(model, i - 1, lit);
		}

		PyObject *ret = Py_BuildValue("O", model);
		Py_DECREF(model);
		return ret;
	}

	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_nof_vars(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	int nof_vars = s->nVars() - 1;  // 0 is a dummy variable

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_vars);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisatgh_nof_cls(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	int nof_cls = s->nClauses();

	PyObject *ret = Py_BuildValue("n", (Py_ssize_t)nof_cls);
	return ret;
}

//
//=============================================================================
static PyObject *py_minisatgh_del(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
	MinisatGH::Solver *s = (MinisatGH::Solver *)pyobj_to_void(s_obj);

	delete s;
	Py_RETURN_NONE;
}

//
//=============================================================================
static PyObject *py_minisatgh_acc_stats(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *s_obj;

	if (!PyArg_ParseTuple(args, "O", &s_obj))
		return NULL;

	// get pointer to solver
#if PY_MAJOR_VERSION < 3
	MinisatGH::Solver *s = (MinisatGH::Solver *)PyCObject_AsVoidPtr(s_obj);
#else
	MinisatGH::Solver *s = (MinisatGH::Solver *)PyCapsule_GetPointer(s_obj, NULL);
#endif

	return Py_BuildValue("{s:l,s:l,s:l,s:l}",
		"restarts", s->starts,
		"conflicts", s->conflicts,
		"decisions", s->decisions,
		"propagations", s->propagations
	);
}
#endif  // WITH_MINISATGH

}  // extern "C"
