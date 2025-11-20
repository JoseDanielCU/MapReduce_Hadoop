from mrjob.job import MRJob
import csv

class AvgTemp(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--start", help="Fecha inicio YYYY-MM-DD", default="2022-01-01")
        self.add_passthru_arg("--end", help="Fecha fin YYYY-MM-DD", default="2023-01-01")

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))

            if row[0] == "time":
                return

            fecha = row[0].split("T")[0]
            temp = float(row[1])

            if self.options.start <= fecha <= self.options.end:
                yield fecha, temp

        except:
            pass

    def reducer(self, fecha, temps):
        temps_list = list(temps)
        yield fecha, sum(temps_list) / len(temps_list)


if __name__ == "__main__":
    AvgTemp.run()
