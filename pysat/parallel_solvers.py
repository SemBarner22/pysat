import logging
import queue
import subprocess
from pysat.solvers import Solver
import multiprocessing as mp
import tempfile

def run_solver(name, clauses, assumptions):
    """
        Run a single solver on a given formula.
    """
    solver = Solver(name)
    for clause in clauses:
        solver.add_clause(list(clause))
    res = solver.solve(assumptions)
    return res, solver.get_model()


class ParallelSolverPortfolio(Solver):

    def __init__(self, names, **kwargs):
        super().__init__(**kwargs)
        self.solvers = names
        import multiprocessing as mp
        self.q = queue.Queue()
        self.p = mp.Pool(len(names))
        self.names = names
        self.model = None
        self.result = False
        self.model = []
        self.ress = False
        self.list_of_clauses = []
        self.amount_of_variables = 0

    def new(self, name='custom', bootstrap_with=None, use_timer=False, **kwargs):
        pass

    def quit(self, i):
        self.ress, self.model = i
        self.p.terminate()
        self.p = mp.Pool(len(self.names))
        self.q.put("")

    def solve(self, assumptions=[]):
        logging.info("Parallel solving started")
        logging.info("Creating tasks")

        for i in range(len(self.solvers)):
            self.p.apply_async(run_solver, args=(self.solvers[i], self.list_of_clauses, assumptions), callback=self.quit)
        self.q.get(block=True)
        self.result = self.ress
        logging.info("Main Ended")

        return self.result

    def append_formula(self, formula, **kwargs):
        for clause in formula:
            self.list_of_clauses.append(clause)

    def add_clause(self, clause, **kwargs):
        for c in clause:
            self.amount_of_variables = max(self.amount_of_variables, abs(c))
        self.list_of_clauses.append(clause)

    def nof_vars(self):
        return self.amount_of_variables

    def nof_clauses(self):
        return len(self.list_of_clauses)

    def get_model(self):
        return self.model


class ParallelSolverPathToFile(Solver):

    def __init__(self, path_to_script, working_directory, path_to_file=None):
        super(ParallelSolverPathToFile, self).__init__("")
        self.list_of_clauses = []
        self.amount_of_variables = 0
        self.result = False
        self.answer = None
        self.path_script = path_to_script
        self.path_file = path_to_file
        self.working_directory = working_directory

    def new(self, name='custom', bootstrap_with=None, use_timer=False, **kwargs):
        pass

    def get_model(self):
        r = self.result
        ans = self.answer
        result = []
        if r:
            for a in ans:
                a = a[2:]
                print(str(a))
                res = [int(x) for x in a.split(' ')]
                try:
                    res.remove(0)
                except ValueError:
                    pass
                result.extend(res)
            print(str(result))
            return result
        else:
            print("No answer")

    @staticmethod
    def write_to_file(self, file):
        file.write("c .cnf file\n")
        file.write("p cnf " + str(self.amount_of_variables) + " " + str(len(self.list_of_clauses)) + "\n")
        for clause in self.list_of_clauses:
            file.write(" ".join(str(x) for x in clause) + " 0" + " \n")
        file.flush()

    # @staticmethod
    def execute(self, path_script, working_directory, tmp=None):
        # input - аргументы к испольняемому запросу
        if self.path_file is not None:
            exit_code = subprocess.run([path_script, str(tmp.name)],
                                  shell=False, capture_output=True, text=True,
                                  cwd=working_directory)
            result = exit_code.stdout.split("\n")
        else:
            process = subprocess.Popen([path_script],
                                       stdout=subprocess.PIPE,
                                       stdin=subprocess.PIPE,
                                       text=True,
                                       shell=True,
                                       cwd=working_directory)
            s = "c A sample .cnf file\n"
            s += "p cnf " + str(self.amount_of_variables) + " " + str(len(self.list_of_clauses)) + "\n"
            for clause in self.list_of_clauses:
                s += " ".join(str(x) for x in clause) + " 0" + " \n"
            stdout, stderr = process.communicate(input=s)
            result = stdout.split("\n")
        is_sat = list(filter(lambda s: s.startswith('s'), result))
        assignments = list(filter(lambda v: v.startswith('v'), result))
        if is_sat[0] == "s SATISFIABLE":
            return True, assignments
        else:
            return False, None

    # currently without assumptions
    def solve(self, assumptions=[]):
        if self.path_file is not None:
            with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8', suffix='.cnf') as tmp:
                self.write_to_file(tmp)
                self.result, self.answer = self.execute(self.path_script, self.working_directory, tmp)
        self.result, self.answer = self.execute(self.path_script, self.working_directory)
        return self.result

    def add_clause(self, clause):
        for c in clause:
            self.amount_of_variables = max(self.amount_of_variables, abs(c))
        self.list_of_clauses.append(clause)

    def append_formula(self, formula):
        for clause in formula:
            self.add_clause(clause)

    def nof_vars(self):
        return self.amount_of_variables

    def nof_clauses(self):
        return len(self.list_of_clauses)