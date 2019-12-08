package transform

type Transformer interface {
	Transform(string) (string, error)
	Info()
}