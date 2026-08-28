use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders};

/// Inherit the terminal foreground. Gray, DarkGray, White, and Dim vanish on light terminals.
pub(crate) fn body_style() -> Style {
    Style::default()
}

pub(crate) fn accent() -> Style {
    Style::default().fg(Color::Blue)
}

pub(crate) fn accent_bold() -> Style {
    accent().add_modifier(Modifier::BOLD)
}

/// Blue row highlight paints both fg and bg so it does not depend on the terminal theme.
pub(crate) fn highlight() -> Style {
    Style::default()
        .fg(Color::White)
        .bg(Color::Blue)
        .add_modifier(Modifier::BOLD)
}

/// Unfocused row selection: painted background so it stays visible on light terminals.
pub(crate) fn idle_highlight() -> Style {
    Style::default().fg(Color::White).bg(Color::Gray)
}

pub(crate) fn block(title: &str, focused: bool) -> Block<'_> {
    Block::default()
        .title(Span::styled(title, body_style()))
        .borders(Borders::ALL)
        .border_style(if focused { accent() } else { body_style() })
}

#[cfg(test)]
mod tests {
    use super::{accent, body_style, highlight, idle_highlight};
    use ratatui::style::{Color, Style};

    #[test]
    fn palette_inherits_terminal_foreground() {
        assert_eq!(body_style(), Style::default());
        assert_eq!(accent().fg, Some(Color::Blue));
        assert_eq!(highlight().fg, Some(Color::White));
        assert_eq!(highlight().bg, Some(Color::Blue));
        assert_eq!(idle_highlight().fg, Some(Color::White));
        assert_eq!(idle_highlight().bg, Some(Color::Gray));
    }
}
