package chbackup

import (
	"io"

	progressbar "github.com/cheggaaa/pb/v3"
)

type Bar struct {
	pb   *progressbar.ProgressBar
	show bool
}

func StartNewByteBar(show bool, total int64) *Bar {
	if show {
		return &Bar{
			show: true,
			pb:   progressbar.Start64(total),
		}
	}
	return &Bar{
		show: false,
	}
}

func (b *Bar) Finish() {
	if b.show {
		b.pb.Finish()
	}
}

func (b *Bar) Add64(add int64) {
	if b.show {
		b.pb.Add64(add)
	}
}

func (b *Bar) Increment() {
	if b.show {
		b.pb.Increment()
	}
}

func (b *Bar) NewProxyReader(r io.Reader) io.Reader {
	if b.show {
		return b.pb.NewProxyReader(r)
	}
	return r
}
