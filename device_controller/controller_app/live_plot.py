"""Reuse static Matplotlib axes when updating a live sensor curve."""


class LiveLineRenderer:
    def __init__(self, canvas, axis, line, on_frame):
        self.canvas = canvas
        self.axis = axis
        self.line = line
        self.on_frame = on_frame
        self.background = None
        self._limits = None
        self.line.set_animated(True)
        canvas.mpl_connect('draw_event', self._on_draw)
        canvas.mpl_connect('resize_event', self._invalidate)
        axis.callbacks.connect('xlim_changed', self._invalidate)
        axis.callbacks.connect('ylim_changed', self._invalidate)

    def _invalidate(self, _event=None):
        self.background = None

    def _on_draw(self, _event):
        self.background = self.canvas.copy_from_bbox(self.axis.bbox)
        self.axis.draw_artist(self.line)
        self.canvas.blit(self.axis.bbox)
        self.on_frame()

    def render(self, x, y, *, xlim, ylim, title):
        self.line.set_data(x, y)
        limits = (xlim, ylim, title)
        if limits != self._limits:
            if tuple(self.axis.get_xlim()) != xlim:
                self.axis.set_xlim(*xlim)
            if tuple(self.axis.get_ylim()) != ylim:
                self.axis.set_ylim(*ylim)
            if self.axis.get_title() != title:
                self.axis.set_title(title)
            self._limits = limits
            self.background = None
        if self.background is None:
            self.canvas.draw_idle()
        else:
            self.canvas.restore_region(self.background)
            self.axis.draw_artist(self.line)
            self.canvas.blit(self.axis.bbox)
            self.on_frame()
