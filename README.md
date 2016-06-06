# MPCite

```
Continuous and High-Throughput Allocation of Digital Object Identifiers
for computed and contributed Materials Data in the Materials Project
        - Accepted as invited talk at “Reproducibility" mini-symposium of
          SciPy16 (http://scipy2016.scipy.org/ehome/146062/332963/)
```

## Brief Description

“MPCite” enables the continuous request, validation, and dissemination of
Digital Object Identifiers (DOIs) for all inorganic materials currently
available in the Materials Project (MP, www.materialsproject.org). The library
provides MP's users with the necessary software infrastructure to achieve a new
level of reproducibility in their research: (i) convenient and persistent
citation of MP's materials data in online and print publications, and (ii)
facilitated sharing amongst collaborators. "MPCite" can also be employed for
the assignment of DOIs to non-core database entries such as theoretical and
experimental data contributed through "MPContribs" or suggested by the user for
calculation via the “MPComplete” service. The fundamental principle underlying
MPCite can easily be extended to other scientific domains where the number of
data records demands high-throughput and continuous allocation of DOIs.

## Long Description

The new open-source software package, “MPCite” [1] enables the continuous
request, validation, and dissemination of Digital Object Identifiers (DOIs) for
all >70k inorganic materials currently available in the Materials Project (MP,
www.materialsproject.org) database. Materials defined by a set of similar
inorganic crystal structures are a good match for DOIs because they have a
unique and stable definition. The functionality provided by MPCite is
increasingly important in support of “MPComplete”, a service where users
suggest new compounds for which MP will calculate detailed electronic structure
properties. MPComplete then automatically integrates the results of each
calculation with MP’s core dataset. Users are increasingly willing to delegate
computation to MP because they quickly get reproducible results from a trusted
analysis pipeline with DOIs they can cite in their follow-up analysis papers.

The DOE Office of Scientific and Technical Information (OSTI) [2] provides the
E-Link service and programming interface free of charge to DOE-funded
scientific projects. It allows researchers to submit information about OSTI
products (in form of XML meta-data records) and retrieve a persistent DOI to
identify it on the world wide web. DOIs are most commonly used for referencing
and locating journal papers because they provide a unique URL linking to the
journal’s online landing page with more information about the publication. The
landing page might change over time, but the DOI - once requested - is
immutable.

With MPCite, we are expanding and applying the use of DOIs from papers, reports
and small singular/static datasets to the ever-growing set of materials data
available in MP. For OSTI, the tens of thousands of requests from a single
client constitutes an unprecedented scale. The resulting workload can only be
managed with a continuously running task manager which sends requests to OSTI
in chunks to initially achieve full DOI coverage within a few months. Not only
does the manager subsequently keep requesting DOIs as new materials become
available, it also assures the propagation of updates in materials data to OSTI
without duplicating DOIs. To support such a “high-throughput” mode, MPCite
includes self-healing error handlers and monitoring capabilities that are
usually not required when dealing with up to a few dozen DOI requests and one
entry at a time. Another integral task of the DOI manager is the automated
generation of BibTeX strings for each material, which are also used to validate
that the DOIs successfully resolve to the appropriate landing page. This
functionality is exposed to the user on the materials details page in our
portal [3]. MPCite interactively live-monitors the overall status of requested
versus validated DOIs in comparison to the total number of materials through
Plotly’s Streaming API [4]. In recognition that user analyses will often use
many related materials, the user can also manually request a representative DOI
through our portal to reference a collection of materials used in his analysis,
or to share it with collaborators.

In summary, our efforts to assign DOIs to all materials available in MP
provides our users with the necessary software infrastructure to achieve a new
level of reproducibility in their research. This is not only evident in the
convenient and persistent citation of our materials data in online and print
publications, but also in the facilitated sharing amongst collaborators. In the
future, we plan to extend the use of DOIs to non-core database entries such as
theoretical and experimental data contributed by our users through "MPContribs"
[5]. Once established in MP, MPCite can also be easily extended to other
scientific domains where the number of data records demands the high-throughput
and continuous allocation of DOIs.

[1] MPCite, https://github.com/materialsproject/MPCite  
[2] OSTI, https://www.osti.gov  
[3] Example Materials Detail Page for As (mp-10), http://dx.doi.org/10.17188/1184812  
[4] Plotly, https://plot.ly  
[5] MPContribs, arXiv:1510.05024, arXiv:1510.05727, MRS Spring 2016
