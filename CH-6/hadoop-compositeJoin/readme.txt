Composite Join.

La solución lo que hace es utilizar la clase KeyValueTextInputFormat como parser de la entrada, al cual por defecto utiliza documentos separados con tab, en donde la clave foranea a utilizar es la primera de cada documento.

Por ejemplo, en las pruebas iniciales realizadas se utilizaron los siguientes conjuntos de datos:

users.txt:
1	Jim
2	Tom
3	Harry
4	Richa

details.txt:
1	Descripcion 1
2	Descripcion 2
3	Descripcion 3
4	Descripcion 4
5	Descripcion 5

output.txt
Jim     Descripcion 1
Tom     Descripcion 2
Harry   Descripcion 3
Richa   Descripcion 4

Mas allá de que los joins son operaciones no especialmente idóneas para la infraestructura de hadoop, la implementación básica de un composite join es provista por hadoop, pudiendo realizarse un inner join o outer join con algunas salvedades:

1 - En cada split de los datos de entrada, las claves a utilizar deben estar en el mismo split y deben estar ordenadas.
2 - Ambos conjuntos de datos tienen la misma cantidad de particiones.

En código principal seta como se van a pardear los inputs utilizando KeyValueTextInputFormat y especificando que tipo de join utilizar "inner" o "outer". El framework se encarga de la aplicación del join. El mapper únicamente recibe los datos de entrada basados en el key (por defecto la primer columna de archivos separados por tab) y los imprime como output. En este ejemplo, recibirá "Jim" y "Descripcion 1" e imprimida "Jim"\t"Descripcion 1".

En este caso, no tiene sentido tener combines o reduces, ya que las salidas de cada maper dependen de las keys existentes en las particiones de entrada, por lo cual no pueden ser reducidas ni combinadas.


