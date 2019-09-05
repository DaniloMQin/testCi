class Conto:
    def __init__(self, nome, conto):
        self.nome = nome
        self.conto = conto

class ContoCorrente(Conto):
    def __init__(self, nome, conto, importo):
        super().__init__(nome, conto)
        self.__saldo = importo

    def preleva(self, importo):
        self.__saldo -= importo 

    def deposita(self, importo):
        self.__saldo += importo

    def descrizione(self):
        print(self.nome, self.conto, 
        self.saldo)
    @property
    def saldo(self):
        return self.__saldo

    @saldo.setter
    def saldo(self, importo):
        self.preleva(self.__saldo)
        self.deposita(importo)

# Aligned with opening delimiter.
class GestoreContiCorrente:
    @staticmethod
    def bonifico(sorgente, destinazione, importo):
        sorgente.preleva(importo)
        destinazione.deposita(importo)
      
C1 = ContoCorrente("Danilo", "10", 2000)
C2 = ContoCorrente("Suso", "20", 5300)

C1.descrizione()
C2.descrizione()

GestoreContiCorrente.bonifico(C1, C2, 500)

C1.descrizione()
C2.descrizione()
