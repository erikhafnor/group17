# Builds main.pdf. Uses latexmk when installed (as the Report workflow does),
# otherwise runs the pdflatex/bibtex sequence directly.
SOURCES := main.tex references.bib $(wildcard sections/*.tex)
LATEX := pdflatex -interaction=nonstopmode -halt-on-error -file-line-error

main.pdf: $(SOURCES)
ifneq ($(shell command -v latexmk 2>/dev/null),)
	latexmk -pdf -file-line-error -halt-on-error -interaction=nonstopmode main.tex
else
	$(LATEX) main.tex
	bibtex main
	$(LATEX) main.tex
	$(LATEX) main.tex
endif

.PHONY: clean
clean:
	rm -f *.aux *.bbl *.blg *.fdb_latexmk *.fls *.log *.out *.synctex.gz *.toc main.pdf
