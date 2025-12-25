package utils

func NewPath(s ...string) Path {
	p := Path{}
	p = append(p, s...)
	return p
}

type Path []string

func (p *Path) AddString(s ...string) Path {
	return append(*p, s...)
}

func (p *Path) Export() []string {
	return *p
}

func (p Path) First() (string, bool) {
	if len(p) == 0 {
		return "", false
	}
	return p[0], true
}

func (p Path) Next() Path {
	if len(p) == 0 {
		return Path{}
	}
	return p[1:]
}
